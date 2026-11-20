/*
 * Copyright 2020-2025 Typelevel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package cats.effect.ffm.benchmarks

import cats.effect.ffm.internal.Libc

import org.openjdk.jmh.annotations._

import java.lang.foreign.{Arena, MemorySegment, ValueLayout}
import java.nio.ByteBuffer
import java.nio.channels.{Pipe, Selector}
import java.util.concurrent.TimeUnit

/**
 * Cost of a single downcall through FFM, by linker option, against the JNI path NIO uses.
 *
 * {{{
 * ffmBenchmarks/Jmh/run -i 5 -wi 3 -f 1 -t 1 cats.effect.ffm.benchmarks.DowncallBenchmark
 * }}}
 *
 *   - `abs*`: a libc function that makes no syscall, isolating the transition cost.
 *   - `getppid*`: the cheapest real syscall.
 *   - `epollWaitNow*` / `selectNow`: a non-blocking poll of an empty epoll set (what a worker does
 *     between fibers) via FFM vs `java.nio.channels.Selector`.
 *   - `readEmpty*`: a read that fails with EAGAIN (an empty non-blocking pipe).
 *   - `roundTrip*`: write `size` bytes into a pipe and read them back, from native memory, from a
 *     heap `byte[]` via a critical heap-access downcall, and through NIO with direct and heap
 *     buffers (NIO copies heap buffers through a temporary direct buffer).
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(
  value = 1,
  jvmArgsAppend = Array("--enable-native-access=ALL-UNNAMED", "-Xms1g", "-Xmx1g"))
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
class DowncallBenchmark {

  @Param(Array("64", "16384"))
  var size: Int = _

  private[this] var arena: Arena = _
  private[this] var cap: MemorySegment = _
  private[this] var epfd: Int = _
  private[this] var events: MemorySegment = _
  private[this] var rfd, wfd: Int = _
  private[this] var native: MemorySegment = _
  private[this] var heapArr: Array[Byte] = _
  private[this] var heapSeg: MemorySegment = _

  private[this] var selector: Selector = _
  private[this] var pipe: Pipe = _
  private[this] var nioDirect: ByteBuffer = _
  private[this] var nioHeap: ByteBuffer = _

  @Setup
  def setup(): Unit = {
    arena = Arena.ofShared()
    cap = Libc.allocateCapture(arena)
    epfd = Libc.epollCreate1(0)
    events = arena.allocate(Libc.EPOLL_EVENT_SIZE * 64, 8L)
    val fds = arena.allocate(ValueLayout.JAVA_INT, 2L)
    if (Libc.pipe2(fds, Libc.SOCK_NONBLOCK | Libc.SOCK_CLOEXEC) != 0) sys.error("pipe2")
    rfd = fds.getAtIndex(ValueLayout.JAVA_INT, 0L)
    wfd = fds.getAtIndex(ValueLayout.JAVA_INT, 1L)
    native = arena.allocate(size.toLong, 64L)
    heapArr = new Array[Byte](size)
    heapSeg = MemorySegment.ofArray(heapArr)

    selector = Selector.open()
    pipe = Pipe.open()
    pipe.source().configureBlocking(false)
    pipe.sink().configureBlocking(false)
    nioDirect = ByteBuffer.allocateDirect(size)
    nioHeap = ByteBuffer.allocate(size)
  }

  @TearDown
  def tearDown(): Unit = {
    Libc.close(rfd); Libc.close(wfd); Libc.close(epfd)
    arena.close()
    selector.close(); pipe.source().close(); pipe.sink().close()
  }

  // --- pure transition cost --------------------------------------------------------------------

  @Benchmark def absPlain(): Int = Libc.abs(-size)
  @Benchmark def absCritical(): Int = Libc.absC(-size)
  @Benchmark def absCapture(): Int = Libc.absE(cap, -size)
  @Benchmark def absCriticalCapture(): Int = Libc.absCE(cap, -size)

  // --- trivial syscall -------------------------------------------------------------------------

  @Benchmark def getppidPlain(): Int = Libc.getppid()
  @Benchmark def getppidCritical(): Int = Libc.getppidC()

  // --- non-blocking poll of an empty set -------------------------------------------------------

  @Benchmark def epollWaitNowPlain(): Int = Libc.epollWait(cap, epfd, events, 64, 0)
  @Benchmark def epollWaitNowCritical(): Int = Libc.epollWaitNow(cap, epfd, events, 64)
  @Benchmark def selectNow(): Int = selector.selectNow()

  // --- read that hits EAGAIN -------------------------------------------------------------------

  @Benchmark def readEmptyPlain(): Long = Libc.readE(cap, rfd, native, size.toLong)
  @Benchmark def readEmptyCritical(): Long = Libc.readCE(cap, rfd, native, size.toLong)
  @Benchmark def readEmptyNio(): Int = { nioDirect.clear(); pipe.source().read(nioDirect) }

  // --- write + read `size` bytes ---------------------------------------------------------------

  @Benchmark def roundTripFfmNativePlain(): Long = {
    Libc.writeE(cap, wfd, native, size.toLong)
    Libc.readE(cap, rfd, native, size.toLong)
  }

  @Benchmark def roundTripFfmNativeCritical(): Long = {
    Libc.writeCE(cap, wfd, native, size.toLong)
    Libc.readCE(cap, rfd, native, size.toLong)
  }

  /** Heap `byte[]` passed straight to the kernel: no copy, no direct buffer. */
  @Benchmark def roundTripFfmHeapCritical(): Long = {
    Libc.writeCHE(cap, wfd, heapSeg, size.toLong)
    Libc.readCHE(cap, rfd, heapSeg, size.toLong)
  }

  /** Heap `byte[]` the portable way: copy through native memory both directions. */
  @Benchmark def roundTripFfmHeapCopy(): Long = {
    MemorySegment.copy(heapSeg, 0L, native, 0L, size.toLong)
    Libc.writeCE(cap, wfd, native, size.toLong)
    val n = Libc.readCE(cap, rfd, native, size.toLong)
    MemorySegment.copy(native, 0L, heapSeg, 0L, n)
    n
  }

  @Benchmark def roundTripNioDirect(): Int = {
    nioDirect.clear(); pipe.sink().write(nioDirect)
    nioDirect.clear(); pipe.source().read(nioDirect)
  }

  @Benchmark def roundTripNioHeap(): Int = {
    nioHeap.clear(); pipe.sink().write(nioHeap)
    nioHeap.clear(); pipe.source().read(nioHeap)
  }
}
