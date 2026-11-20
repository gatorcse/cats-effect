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
package net

import cats.effect.{FileDescriptorPollHandle, FileDescriptorPoller, IO, Resource}
import cats.effect.EdgeTriggeredPollHandle
import cats.effect.ffm.benchmarks.{BenchLibc => Libc}
import cats.syntax.all._

import java.io.IOException
import java.lang.foreign.{Arena, MemorySegment, ValueLayout}

/**
 * Spike-quality, Linux-only TCP sockets on raw file descriptors, driven by `FfmEpollSystem`.
 * Enough to run an echo server for phase 0 benchmarks; not a public API proposal.
 */
object Tcp {

  /** Per-thread errno capture segment. Leaked per thread; fine for a spike. */
  private[this] val captureTL: ThreadLocal[MemorySegment] =
    ThreadLocal.withInitial(() => Libc.allocateCapture(Arena.global()))

  @inline private[net] def capture(): MemorySegment = captureTL.get()

  final val ScratchSize = 16384

  /** Per-thread native scratch buffer for reads that are copied onto the heap. */
  private[this] val scratchTL: ThreadLocal[MemorySegment] =
    ThreadLocal.withInitial(() => Arena.global().allocate(ScratchSize.toLong, 64L))

  @inline private[net] def scratch(): MemorySegment = scratchTL.get()

  /** Opens a non-blocking listening socket on 0.0.0.0:port. */
  def listen(port: Int, backlog: Int = 4096): Resource[IO, ServerSocket] =
    Resource.make(IO {
      val fd = Libc.socket(Libc.AF_INET, Libc.SOCK_STREAM | Libc.SOCK_NONBLOCK | Libc.SOCK_CLOEXEC, 0)
      if (fd < 0) throw new IOException("socket failed")
      val arena = Arena.ofConfined()
      try {
        val one = arena.allocate(ValueLayout.JAVA_INT)
        one.set(ValueLayout.JAVA_INT, 0L, 1)
        Libc.setsockopt(fd, Libc.SOL_SOCKET, Libc.SO_REUSEADDR, one, 4)
        val addr = arena.allocate(Libc.SOCKADDR_IN)
        addr.set(ValueLayout.JAVA_SHORT, 0L, Libc.AF_INET.toShort)
        addr.set(ValueLayout.JAVA_SHORT, 2L, java.lang.Short.reverseBytes(port.toShort))
        addr.set(ValueLayout.JAVA_INT, 4L, 0) // INADDR_ANY
        if (Libc.bind(fd, addr, Libc.SOCKADDR_IN.byteSize().toInt) != 0)
          throw new IOException(s"bind($port) failed")
        if (Libc.listen(fd, backlog) != 0) throw new IOException("listen failed")
      } finally arena.close()
      fd
    })(fd => IO { Libc.close(fd); () }).flatMap { fd =>
      Resource.eval(FileDescriptorPoller.get).flatMap { poller =>
        poller.registerFileDescriptor(fd, true, false).map(h => new ServerSocket(fd, h))
      }
    }

  final class ServerSocket private[Tcp] (val fd: Int, handle: FileDescriptorPollHandle) {

    /** Accepts one connection, returning its registered socket. */
    def accept: Resource[IO, Socket] =
      Resource
        .makeFull[IO, Int](poll => poll(handle.pollReadRec(()) { _ =>
          IO {
            val cfd = Libc.accept4(capture(), fd, Libc.SOCK_NONBLOCK | Libc.SOCK_CLOEXEC)
            if (cfd >= 0) Right(cfd)
            else {
              val err = Libc.errno(capture())
              if (err == Libc.EAGAIN) Left(())
              else throw new IOException(s"accept4 failed: errno $err")
            }
          }
        }))(cfd => IO { Libc.close(cfd); () })
        .flatMap { cfd =>
          Resource.eval(IO(setNoDelay(cfd))) >>
            Resource.eval(FileDescriptorPoller.get).flatMap { poller =>
              poller.registerFileDescriptor(cfd, true, true).map(h => new Socket(cfd, h))
            }
        }
  }

  private def setNoDelay(fd: Int): Unit = {
    val arena = Arena.ofConfined()
    try {
      val one = arena.allocate(ValueLayout.JAVA_INT)
      one.set(ValueLayout.JAVA_INT, 0L, 1)
      Libc.setsockopt(fd, Libc.IPPROTO_TCP, Libc.TCP_NODELAY, one, 4)
      ()
    } finally arena.close()
  }

  /**
   * Spike knob: when a read returns fewer bytes than requested the socket is drained, so the next
   * read waits for a new edge instead of first trying (and failing with `EAGAIN`). Disable with
   * `-Dcats.effect.ffm.shortReadHint=false`.
   */
  private val shortReadHint: Boolean =
    !"false".equals(System.getProperty("cats.effect.ffm.shortReadHint"))

  final class Socket private[Tcp] (val fd: Int, handle0: FileDescriptorPollHandle) {

    private[this] val handle: EdgeTriggeredPollHandle = handle0.asInstanceOf[EdgeTriggeredPollHandle]

    /** Read-ready count observed just before a short read; -1 if not known to be drained. */
    @volatile private[this] var drainedAt: Int = -1

    /**
     * Single-reader fast path: waits for a fresh edge if the last read drained the socket, then
     * tries the read once; falls back to the generic retry loop on `EAGAIN`.
     */
    def readHinted(buf: MemorySegment, len: Long): IO[Int] = {
      val seen = drainedAt
      val ready = if (seen >= 0) handle.awaitReadReady(seen) else IO.unit
      ready >> IO {
        val before = handle.readReadyCount
        val cap = capture()
        val n = Libc.readCE(cap, fd, buf, len)
        if (n > 0) {
          drainedAt = if (n < len) before else -1
          n.toInt
        } else if (n == 0) -1
        else {
          val err = Libc.errno(cap)
          if (err == Libc.EAGAIN) {
            drainedAt = before
            Int.MinValue
          } else throw new IOException(s"read failed: errno $err")
        }
      }.flatMap { n => if (n == Int.MinValue) readHinted(buf, len) else IO.pure(n) }
    }

    def readArrayHinted(max: Int): IO[Array[Byte]] = {
      val seen = drainedAt
      val ready = if (seen >= 0) handle.awaitReadReady(seen) else IO.unit
      ready >> IO {
        val before = handle.readReadyCount
        val cap = capture()
        val buf = scratch()
        val len = math.min(max, ScratchSize)
        val n = Libc.readCE(cap, fd, buf, len.toLong)
        if (n > 0) {
          drainedAt = if (n < len) before else -1
          val out = new Array[Byte](n.toInt)
          MemorySegment.copy(buf, ValueLayout.JAVA_BYTE, 0L, out, 0, n.toInt)
          out
        } else if (n == 0) null
        else {
          val err = Libc.errno(cap)
          if (err == Libc.EAGAIN) {
            drainedAt = before
            Array.emptyByteArray
          } else throw new IOException(s"read failed: errno $err")
        }
      }.flatMap { a =>
        if ((a ne null) && a.length == 0) readArrayHinted(max) else IO.pure(a)
      }
    }

    /**
     * Reads up to `len` bytes into `buf` (native or, with `heap = true`, a heap segment such as
     * `MemorySegment.ofArray(bytes)`), suspending until readable. Returns -1 at EOF.
     */
    def read(buf: MemorySegment, len: Long): IO[Int] =
      if (shortReadHint && buf.isNative()) readHinted(buf, len)
      else readGeneric(buf, len)

    private def readGeneric(buf: MemorySegment, len: Long): IO[Int] =
      handle.pollReadRec(()) { _ =>
        IO {
          val cap = capture()
          val n =
            if (buf.isNative()) Libc.readCE(cap, fd, buf, len)
            else Libc.readCHE(cap, fd, buf, len)
          if (n > 0) Right(n.toInt)
          else if (n == 0) Right(-1)
          else {
            val err = Libc.errno(cap)
            if (err == Libc.EAGAIN) Left(())
            else throw new IOException(s"read failed: errno $err")
          }
        }
      }

    /**
     * Reads up to `max` bytes into a freshly allocated, exactly sized `byte[]` (the shape of an
     * fs2 `Chunk`), via this thread's native scratch buffer. Returns `null` at EOF.
     */
    def readArray(max: Int): IO[Array[Byte]] =
      if (shortReadHint) readArrayHinted(max) else readArrayGeneric(max)

    private def readArrayGeneric(max: Int): IO[Array[Byte]] =
      handle.pollReadRec(()) { _ =>
        IO {
          val cap = capture()
          val buf = scratch()
          val n = Libc.readCE(cap, fd, buf, math.min(max, ScratchSize).toLong)
          if (n > 0) {
            val out = new Array[Byte](n.toInt)
            MemorySegment.copy(buf, ValueLayout.JAVA_BYTE, 0L, out, 0, n.toInt)
            Right(out)
          } else if (n == 0) Right(null)
          else {
            val err = Libc.errno(cap)
            if (err == Libc.EAGAIN) Left(())
            else throw new IOException(s"read failed: errno $err")
          }
        }
      }

    /** Writes all `len` bytes of `buf`, suspending while the socket is not writable. */
    def writeAll(buf: MemorySegment, len: Long): IO[Unit] =
      handle.pollWriteRec(0L) { off =>
        IO {
          val cap = capture()
          var o = off
          var again = false
          while (o < len && !again) {
            val slice = buf.asSlice(o, len - o)
            val n =
              if (buf.isNative()) Libc.writeCE(cap, fd, slice, len - o)
              else Libc.writeCHE(cap, fd, slice, len - o)
            if (n >= 0) o += n
            else {
              val err = Libc.errno(cap)
              if (err == Libc.EAGAIN) again = true
              else throw new IOException(s"write failed: errno $err")
            }
          }
          if (o >= len) Right(()) else Left(o)
        }
      }
  }
}
