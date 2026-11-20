# FFM and Native polling benchmarks

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

## Backends

- JVM (`cats.effect.ffm.benchmarks.EchoServer`): `ffm-native`, `ffm-heap`, `nio`, `nio-heap`.
  Add `+nohint` to an `ffm-*` backend to go through the generic locked `pollReadRec` instead of
  the "short read means drained" fast path.
- Scala Native (`native-benchmarks/`, binary copied to `/opt/tools/native-echo`): `native`,
  `native-heap`, `native-nohint`. Same shared epoll loop as the JVM, through `@extern`.
- References: `cecho` (plain C epoll) and `tokio` (2 worker threads).

## Running

```sh
# JMH: downcall cost
sbt "++2.13; ffmBenchmarks/Jmh/run .*DowncallBenchmark.*"

# Echo: build the pieces, export classpaths, then run the harness
gcc -O2 -pthread -o /opt/tools/echoload ffm-benchmarks/loadgen/echoload.c
gcc -O2 -pthread -o /opt/tools/cecho ffm-benchmarks/loadgen/cecho.c
(cd ffm-benchmarks/tokio-echo && cargo build --release)
sbt "++2.13; export ffmBenchmarks/Runtime/fullClasspath"   # -> /opt/tools/ffm.cp
sbt "++2.13; nativeBenchmarks/nativeLink" && \
  cp native-benchmarks/target/scala-2.13/cats-effect-native-benchmarks /opt/tools/native-echo
# in the fs2 fork: sbt "++2.13; export benchmark/Runtime/fullClasspath"  # -> /opt/tools/fs2.cp
WARMUP=10 DURATION=15 ffm-benchmarks/loadgen/bench.py \
  cecho,tokio,fs2,nio,nio-heap,ffm-native,ffm-heap,ffm-native+nohint,native,native-heap,native-nohint \
  c1-64B,c64-64B,c1024-64B,c64-16K
```

`bench.py` pins the server to CPUs 0-1 and the load generator to CPUs 2-3 (it expects 4 cores
and `runserver.sh` in `/opt/tools`; see the harness for the exact JVM flags). Per message, it
reports throughput, latency percentiles, server CPU time, heap allocation, read/write syscalls
from `/proc/<pid>/io` and voluntary context switches.
