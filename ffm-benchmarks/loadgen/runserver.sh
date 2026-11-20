#!/bin/bash
# runserver.sh <backend> <port>   (server pinned to CPUs 0-1)
# JVM ffm backends accept suffixes: ffm-native[+mutex][+nohint], ffm-heap[+mutex][+nohint]
source /opt/tools/env.sh
b=$1; p=$2
JVM="java -Xms1g -Xmx1g -XX:+UseParallelGC -Dcats.effect.tracing.mode=none -Decho.port=$p"
case $b in
  cecho) exec taskset -c 0,1 /opt/tools/cecho $p 2 ;;
  tokio) TOKIO_WORKER_THREADS=2 exec taskset -c 0,1 /home/user/cats-effect/ffm-benchmarks/tokio-echo/target/release/tokio-echo $p ;;
  fs2) exec taskset -c 0,1 $JVM -cp "$(cat /opt/tools/fs2.cp)" fs2.benchmark.echo.EchoServer ;;
  *)
    base=${b%%+*}
    props=""
    
    [[ $b == *+nohint* ]] && props="$props -Dcats.effect.ffm.shortReadHint=false"
    exec taskset -c 0,1 $JVM --enable-native-access=ALL-UNNAMED $props -Decho.backend=$base -cp "$(cat /opt/tools/ffm.cp)" cats.effect.ffm.benchmarks.EchoServer ;;
esac
