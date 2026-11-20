#!/usr/bin/env python3
"""Phase 0 echo benchmark harness.

Runs each echo backend pinned to CPUs 0-1, drives it with echoload pinned to CPUs 2-3, and
reports throughput, latency percentiles, server CPU, and (for JVM servers) allocation and GC
per echoed message. With --strace, instead attaches `strace -c -f` for the measurement window
and reports syscalls per message.

usage: bench.py [--strace] [--out results.jsonl] backend[,backend...] scenario[,scenario...]
"""
import json, os, re, socket, subprocess, sys, time, signal

SCENARIOS = {
    # name: (conns, client_threads, msgsize)
    "c1-64B": (1, 1, 64),
    "c64-64B": (64, 2, 64),
    "c1024-64B": (1024, 2, 64),
    "c64-16K": (64, 2, 16384),
}
WARMUP, DURATION = int(os.environ.get("WARMUP", 10)), int(os.environ.get("DURATION", 15))
CLK = os.sysconf("SC_CLK_TCK")

def proc_cpu(pid):
    with open(f"/proc/{pid}/stat") as f:
        parts = f.read().rsplit(")", 1)[1].split()
    return (int(parts[11]) + int(parts[12])) / CLK

def proc_counters(pid):
    """Zero-overhead kernel counters: read/write-family syscalls and context switches."""
    c = {}
    with open(f"/proc/{pid}/io") as f:
        for line in f:
            k, v = line.split(":")
            c[k] = int(v)
    vol = invol = 0
    for t in os.listdir(f"/proc/{pid}/task"):
        try:
            with open(f"/proc/{pid}/task/{t}/status") as f:
                for line in f:
                    if line.startswith("voluntary_ctxt_switches"):
                        vol += int(line.split()[1])
                    elif line.startswith("nonvoluntary_ctxt_switches"):
                        invol += int(line.split()[1])
        except FileNotFoundError:
            pass
    return c["syscr"], c["syscw"], vol, invol

def server_pid(port):
    for _ in range(100):
        out = subprocess.run(["pgrep", "-f", f"(echo.port={port}|cecho {port}|tokio-echo {port})"],
                             capture_output=True, text=True).stdout.split()
        if out:
            return int(out[-1])
        time.sleep(0.1)
    raise RuntimeError("server did not start")

def wait_listen(port):
    for _ in range(300):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("server not listening")

def run(backend, scen, port, use_strace):
    conns, threads, size = SCENARIOS[scen]
    log = open(f"/tmp/claude-0/srv-{backend}-{scen}.log", "w")
    srv = subprocess.Popen(["/opt/tools/runserver.sh", backend, str(port)], stdout=log, stderr=log)
    try:
        wait_listen(port)
        pid = server_pid(port)
        client = subprocess.Popen(["taskset", "-c", "2,3", "/opt/tools/echoload", str(port), str(conns),
                                   str(threads), str(size), str(WARMUP), str(DURATION)],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(WARMUP)
        cpu0 = proc_cpu(pid)
        k0 = proc_counters(pid)
        st = None
        if use_strace:
            st = subprocess.Popen(["strace", "-c", "-f", "-p", str(pid), "-o", f"/tmp/claude-0/strace-{backend}-{scen}.txt"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(DURATION)
        cpu1 = proc_cpu(pid)
        k1 = proc_counters(pid)
        if st:
            st.send_signal(signal.SIGINT); st.wait()
        out, err = client.communicate(timeout=60)
        res = json.loads(out.strip().splitlines()[-1])
        m0 = int(re.search(r"MEASURE_START (\d+)", err).group(1)) * 1000
        m1 = int(re.search(r"MEASURE_END (\d+)", err).group(1)) * 1000
    finally:
        srv.send_signal(signal.SIGKILL); srv.wait()
        subprocess.run(["pkill", "-9", "-f", f"echo.port={port}"])
        log.close()
    res.update(backend=backend, scenario=scen, server_cpu_pct=round((cpu1 - cpu0) / DURATION * 100, 1))
    n = max(res["msgs"], 1)
    res["reads_per_msg"] = round((k1[0] - k0[0]) / n, 2)
    res["writes_per_msg"] = round((k1[1] - k0[1]) / n, 2)
    res["vol_cs_per_msg"] = round((k1[2] - k0[2]) / n, 3)
    res["invol_cs_per_msg"] = round((k1[3] - k0[3]) / n, 3)
    res["cpu_us_per_msg"] = round((cpu1 - cpu0) * 1e6 / max(res["msgs"], 1), 2)
    stats = []
    for line in open(log.name):
        if line.startswith("STAT "):
            _, t, a, g, gm = line.split()
            stats.append((int(t), int(a), int(g), int(gm)))
    win = [s for s in stats if m0 <= s[0] <= m1 + 999]
    if len(win) >= 2:
        (t0, a0, g0, gm0), (t1, a1, g1, gm1) = win[0], win[-1]
        frac = (t1 - t0) / ((m1 - m0) or 1)
        msgs = res["msgs"] * min(frac, 1.0) if frac > 0 else res["msgs"]
        res["alloc_B_per_msg"] = round((a1 - a0) / max(msgs, 1), 1)
        res["alloc_MB_per_s"] = round((a1 - a0) / ((t1 - t0) / 1000) / 1e6, 1)
        res["gc_count"] = g1 - g0
        res["gc_ms"] = gm1 - gm0
    if use_strace:
        calls = {}
        for line in open(f"/tmp/claude-0/strace-{backend}-{scen}.txt"):
            p = line.split()
            if len(p) >= 5 and p[0][0].isdigit() and p[-1] != "total":
                try:
                    calls[p[-1]] = int(p[3])
                except ValueError:
                    pass
        tot = sum(calls.values())
        res["syscalls_per_msg"] = round(tot / max(res["msgs"], 1), 2)
        res["syscalls_top"] = {k: round(v / max(res["msgs"], 1), 2)
                               for k, v in sorted(calls.items(), key=lambda kv: -kv[1])[:6]}
    return res

def main():
    args = sys.argv[1:]
    use_strace = "--strace" in args
    args = [a for a in args if a != "--strace"]
    out = "/tmp/claude-0/results.jsonl"
    if "--out" in args:
        i = args.index("--out"); out = args[i + 1]; del args[i:i + 2]
    backends, scens = args[0].split(","), args[1].split(",")
    port = 9200
    with open(out, "a") as f:
        for scen in scens:
            for b in backends:
                port += 1
                r = run(b, scen, port, use_strace)
                print(json.dumps(r), flush=True)
                f.write(json.dumps(r) + "\n")

if __name__ == "__main__":
    main()
