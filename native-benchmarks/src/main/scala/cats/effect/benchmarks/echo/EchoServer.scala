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

package cats.effect
package benchmarks
package echo

import cats.effect.std.Supervisor

import scala.scalanative.libc.stdlib
import scala.scalanative.posix.arpa.inet.htons
import scala.scalanative.posix.errno._
import scala.scalanative.posix.fcntl._
import scala.scalanative.posix.netinet.in._
import scala.scalanative.posix.netinet.inOps._
import scala.scalanative.posix.netinet.tcp.TCP_NODELAY
import scala.scalanative.posix.string.strerror
import scala.scalanative.posix.sys.socket._
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

import java.io.IOException

/**
 * TCP echo server on Scala Native, for the same harness as the JVM `EchoServer` (see
 * `ffm-benchmarks/loadgen`). Usage: `native-echo <port> [backend]`, with the worker count from
 * `CE_WORKERS` (default 2, matching the 2 pinned CPUs).
 *
 *   - `native`: per-connection malloc'd buffer, "short read means drained" hint, no heap copy.
 *   - `native-heap`: a fresh `Array[Byte]` per read (the shape of an fs2 `Chunk`).
 *   - `native-nohint`: like `native`, but through the generic locked `pollReadRec`.
 */
object EchoServer extends IOApp {

  override protected def computeWorkerThreadCount: Int =
    sys.env.get("CE_WORKERS").map(_.toInt).getOrElse(2)

  final val BufSize = 16384

  private def fail(op: String): Nothing =
    throw new IOException(s"$op failed: ${fromCString(strerror(errno))}")

  private def setNonBlocking(fd: Int): Unit =
    if (fcntl(fd, F_SETFL, O_NONBLOCK) != 0) fail("fcntl")

  private def setIntOpt(fd: Int, level: Int, opt: Int): Unit = {
    val one = stackalloc[CInt]()
    !one = 1
    if (setsockopt(fd, level, opt, one.asInstanceOf[Ptr[Byte]], sizeof[CInt].toUInt) != 0)
      fail("setsockopt")
  }

  def run(args: List[String]): IO[ExitCode] = {
    val port = args.headOption.map(_.toInt).getOrElse(9000)
    val backend = args.drop(1).headOption.getOrElse("native")
    IO.println(s"echo backend=$backend port=$port") >>
      serve(port, backend).as(ExitCode.Success)
  }

  private def listen(port: Int): Resource[IO, (Int, FileDescriptorPollHandle)] =
    Resource
      .make(IO {
        val fd = socket(AF_INET, SOCK_STREAM, 0)
        if (fd < 0) fail("socket")
        setNonBlocking(fd)
        setIntOpt(fd, SOL_SOCKET, SO_REUSEADDR)
        val addr = stackalloc[sockaddr_in]()
        addr.sin_family = AF_INET.toUShort
        addr.sin_port = htons(port.toUShort)
        addr.sin_addr.s_addr = 0.toUInt // INADDR_ANY
        if (bind(fd, addr.asInstanceOf[Ptr[sockaddr]], sizeof[sockaddr_in].toUInt) != 0)
          fail("bind")
        if (scala.scalanative.posix.sys.socket.listen(fd, 4096) != 0) fail("listen")
        fd
      })(fd => IO { unistd.close(fd); () })
      .flatMap { fd =>
        Resource
          .eval(FileDescriptorPoller.get)
          .flatMap(_.registerFileDescriptor(fd, true, false))
          .map(h => (fd, h))
      }

  private def accept(lfd: Int, h: FileDescriptorPollHandle): IO[Int] =
    h.pollReadRec(()) { _ =>
      IO {
        val cfd = scala.scalanative.posix.sys.socket.accept(lfd, null, null)
        if (cfd >= 0) {
          setNonBlocking(cfd)
          setIntOpt(cfd, IPPROTO_TCP, TCP_NODELAY)
          Right(cfd)
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) Left(())
        else fail("accept")
      }
    }

  private def serve(port: Int, backend: String): IO[Unit] =
    Supervisor[IO](await = false).use { sup =>
      listen(port).use {
        case (lfd, lh) =>
          FileDescriptorPoller.get.flatMap { poller =>
            IO.uncancelable { poll =>
              poll(accept(lfd, lh)).flatMap { cfd =>
                val conn = poller
                  .registerFileDescriptor(cfd, true, true)
                  .use(h => connection(cfd, h, backend))
                  .guarantee(IO { unistd.close(cfd); () })
                sup.supervise(conn)
              }
            }.foreverM
          }
      }
    }

  private def connection(fd: Int, h: FileDescriptorPollHandle, backend: String): IO[Unit] =
    backend match {
      case "native" => new Conn(fd, h, hint = edgeTriggered(h)).runNative
      case "native-nohint" => new Conn(fd, h, hint = false).runNative
      case "native-heap" => new Conn(fd, h, hint = edgeTriggered(h)).runHeap
      case other => IO.raiseError(new IllegalArgumentException(other))
    }

  /** The hint needs the epoll handle's ready counters; kqueue (macOS) has none. */
  private def edgeTriggered(h: FileDescriptorPollHandle): Boolean =
    h.isInstanceOf[EdgeTriggeredPollHandle]

  private final class Conn(fd: Int, h: FileDescriptorPollHandle, hint: Boolean) {
    private[this] val et = if (hint) h.asInstanceOf[EdgeTriggeredPollHandle] else null

    /** Read-ready count seen before a short read; -1 if not known to be drained. */
    private[this] var drainedAt: Int = -1

    /** One read attempt: n > 0 bytes, -1 at EOF, Int.MinValue on EAGAIN. */
    private def readOnce(buf: Ptr[Byte], len: Int): Int = {
      val before = et.readReadyCount
      val n = unistd.read(fd, buf, len.toCSize)
      if (n > 0) {
        drainedAt = if (n < len) before else -1
        n
      } else if (n == 0) -1
      else if (errno == EAGAIN || errno == EWOULDBLOCK) {
        drainedAt = before
        Int.MinValue
      } else fail("read")
    }

    def read(buf: Ptr[Byte], len: Int): IO[Int] =
      if (hint) {
        val seen = drainedAt
        val ready = if (seen >= 0) et.awaitReadReady(seen).void else IO.unit
        ready >> IO(readOnce(buf, len)).flatMap { n =>
          if (n == Int.MinValue) read(buf, len) else IO.pure(n)
        }
      } else
        h.pollReadRec(()) { _ =>
          IO {
            val n = unistd.read(fd, buf, len.toCSize)
            if (n > 0) Right(n)
            else if (n == 0) Right(-1)
            else if (errno == EAGAIN || errno == EWOULDBLOCK) Left(())
            else fail("read")
          }
        }

    def writeAll(buf: Ptr[Byte], len: Int): IO[Unit] =
      h.pollWriteRec(0) { off =>
        IO {
          var o = off
          var again = false
          while (o < len && !again) {
            val n = unistd.write(fd, buf + o.toLong, (len - o).toCSize)
            if (n >= 0) o += n
            else if (errno == EAGAIN || errno == EWOULDBLOCK) again = true
            else fail("write")
          }
          if (o >= len) Right(()) else Left(o)
        }
      }

    def runNative: IO[Unit] =
      Resource
        .make(IO(stdlib.malloc(BufSize.toCSize)))(p => IO(stdlib.free(p)))
        .use { buf =>
          def loop: IO[Unit] =
            read(buf, BufSize).flatMap { n => if (n < 0) IO.unit else writeAll(buf, n) >> loop }
          loop
        }

    /** Reads into a scratch buffer and copies into a fresh array, as fs2 would. */
    def runHeap: IO[Unit] =
      Resource
        .make(IO(stdlib.malloc(BufSize.toCSize)))(p => IO(stdlib.free(p)))
        .use { scratch =>
          def loop: IO[Unit] =
            read(scratch, BufSize).flatMap { n =>
              if (n < 0) IO.unit
              else {
                val chunk = new Array[Byte](n)
                scala.scalanative.libc.string.memcpy(chunk.atUnsafe(0), scratch, n.toCSize)
                writeAll(chunk.atUnsafe(0), n) >> loop
              }
            }
          loop
        }
  }
}
