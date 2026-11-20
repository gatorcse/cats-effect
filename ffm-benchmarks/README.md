# FFM polling spike: phase 0 benchmarks

Everything here is a spike to decide go/no-go on a Panama (FFM) polling system for cats-effect.
Linux x86_64/aarch64 only, JDK 22+ (tested on Temurin 25.0.1).

## Modules

- `ffm/` (`cats-effect-ffm`): `cats.effect.ffm.EpollSystem`, a `PollingSystem` that calls
  `epoll` directly through FFM. It is a port of the Scala Native `EpollSystem` and exposes the
  same `FileDescriptorPoller` API. `cats.effect.ffm.net.Tcp` is just enough raw-fd TCP to run an
  echo server. `cats.effect.ffm.internal.Libc` holds the downcall handles in plain,
  errno-capturing and critical flavours.
- `ffm-benchmarks/`: JMH `DowncallBenchmark` and the `EchoServer` app with four backends
  (`ffm-native`, `ffm-heap`, `nio`, `nio-heap`).
- `ffm-benchmarks/loadgen/`: `echoload.c`, a closed-loop C load generator with a latency
  histogram; `cecho.c`, a plain C epoll echo server as the "speed of light" reference; and
  `bench.py`, the harness.
- `ffm-benchmarks/tokio-echo/`: tokio reference echo server.
- The fs2 baseline lives in the fs2 fork: `benchmark/src/main/scala/fs2/benchmark/echo/EchoServer.scala`.

Neither project is in the root aggregate, so the default build still works on JDK 8 to 21.

## Spike knobs

- `-Dcats.effect.ffm.handleMutex=true|false`: wrap `pollReadRec`/`pollWriteRec` in a `Mutex`
  per handle, as Scala Native does (default true; the echo harness turns it off unless the
  backend name has `+mutex`).
- `-Dcats.effect.ffm.shortReadHint=true|false`: treat a short read as "drained" and wait for the
  next edge rather than issuing a read that will fail with `EAGAIN` (default true).

## Running

```sh
# JMH: downcall cost
sbt "++2.13; ffmBenchmarks/Jmh/run .*DowncallBenchmark.*"

# Echo: build the pieces, export classpaths, then run the harness
gcc -O2 -pthread -o /opt/tools/echoload ffm-benchmarks/loadgen/echoload.c
gcc -O2 -pthread -o /opt/tools/cecho ffm-benchmarks/loadgen/cecho.c
(cd ffm-benchmarks/tokio-echo && cargo build --release)
sbt "++2.13; export ffmBenchmarks/Runtime/fullClasspath"   # -> /opt/tools/ffm.cp
# in the fs2 fork: sbt "++2.13; export benchmark/Runtime/fullClasspath"  # -> /opt/tools/fs2.cp
WARMUP=10 DURATION=15 ffm-benchmarks/loadgen/bench.py \
  cecho,tokio,fs2,nio,nio-heap,ffm-native,ffm-heap,ffm-native+mutex,ffm-native+nohint \
  c1-64B,c64-64B,c1024-64B,c64-16K
```

`bench.py` pins the server to CPUs 0-1 and the load generator to CPUs 2-3 (it expects 4 cores
and `runserver.sh` in `/opt/tools`; see the harness for the exact JVM flags). Per message, it
reports throughput, latency percentiles, server CPU time, heap allocation, read/write syscalls
from `/proc/<pid>/io` and voluntary context switches.
