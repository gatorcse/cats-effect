#!/bin/bash
# portable-echo.sh <out.jsonl> [server...]
#
# A short, unpinned echo comparison for machines without the Linux harness (bench.py needs /proc
# and taskset), e.g. macOS CI runners. Servers: native (Scala Native echo on the platform's
# default polling system, kqueue on macOS), tokio. Expects the binaries built as in the README:
#   NATIVE_ECHO (default native-benchmarks/target/scala-2.13/cats-effect-native-benchmarks)
#   TOKIO_ECHO  (default ffm-benchmarks/tokio-echo/target/release/tokio-echo)
# Numbers from shared runners are noisy: compare servers within one run, not across runs.
set -euo pipefail
cd "$(dirname "$0")/../.."
out=${1:-echo-portable.jsonl}; shift || true
servers=${*:-native tokio}
NATIVE_ECHO=${NATIVE_ECHO:-native-benchmarks/target/scala-2.13/cats-effect-native-benchmarks}
TOKIO_ECHO=${TOKIO_ECHO:-ffm-benchmarks/tokio-echo/target/release/tokio-echo}
WARMUP=${WARMUP:-3}; DURATION=${DURATION:-10}
load=$(mktemp -d)/echoload
cc -O2 -o "$load" ffm-benchmarks/loadgen/echoload.c -lpthread

port=9100
for server in $servers; do
  for scenario in "c1-64B 1 1 64" "c64-64B 64 2 64" "c256-64B 256 2 64"; do
    set -- $scenario
    port=$((port + 1))
    case $server in
      native) CE_WORKERS=2 "$NATIVE_ECHO" $port native & ;;
      tokio) TOKIO_WORKER_THREADS=2 "$TOKIO_ECHO" $port & ;;
      *) echo "unknown server $server" >&2; exit 2 ;;
    esac
    pid=$!
    for _ in $(seq 100); do nc -z 127.0.0.1 $port 2>/dev/null && break; sleep 0.1; done
    result=$("$load" $port $2 $3 $4 "$WARMUP" "$DURATION" 2>/dev/null) || result='{"error":"echoload failed"}'
    kill $pid 2>/dev/null || true
    wait $pid 2>/dev/null || true
    line="{\"os\":\"$(uname -s)-$(uname -m)\",\"server\":\"$server\",\"scenario\":\"$1\",${result#\{}"
    echo "$line" | tee -a "$out"
  done
done
