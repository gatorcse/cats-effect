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
package ffm
package benchmarks

import cats.effect.ffm.benchmarks.net.Tcp
import cats.effect.std.Supervisor
import cats.effect.unsafe.{PollingSystem, SelectorSystem}

import java.lang.foreign.{Arena, MemorySegment}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, SocketChannel}
import java.net.StandardSocketOptions

/**
 * TCP echo server for phase 0. Pick the backend with `-Decho.backend=`:
 *
 *   - `ffm-native`: FFM epoll, per-connection off-heap buffer, no heap copy at all.
 *   - `ffm-heap`: FFM epoll, read into a native scratch buffer then copy into a fresh `byte[]`
 *     per read (the shape of an fs2 `Chunk`), written back from the heap with a critical
 *     heap-access downcall.
 *   - `nio`: stock cats-effect `SelectorSystem`, raw `SocketChannel`, per-connection direct
 *     buffer (best case for NIO).
 *   - `nio-heap`: `SelectorSystem`, fresh `byte[]` per read written via a heap `ByteBuffer`
 *     (what fs2's `SelectingSocket` does).
 */
object EchoServer extends IOApp.Simple {

  private val backend = System.getProperty("echo.backend", "ffm-native")
  private val port = Integer.getInteger("echo.port", 9000)
  private val BufSize = Integer.getInteger("echo.bufsize", 16384).intValue

  override protected def pollingSystem: PollingSystem =
    if (backend.startsWith("ffm")) FfmEpollSystem() else SelectorSystem()

  def run: IO[Unit] =
    IO(AllocReporter.start()) >>
      IO.println(s"echo backend=$backend port=$port") >>
      (backend match {
        case "ffm-native" => ffm(heap = false)
        case "ffm-heap" => ffm(heap = true)
        case "nio" => nio(heap = false)
        case "nio-heap" => nio(heap = true)
        case other => IO.raiseError(new IllegalArgumentException(other))
      })

  // ---------------------------------------------------------------------------------------------
  // FFM epoll

  private def ffm(heap: Boolean): IO[Unit] =
    Supervisor[IO](await = false).use { sup =>
      Tcp.listen(port).use { server =>
        server
          .accept
          .allocated
          .flatMap {
            case (sock, release) =>
              sup.supervise(ffmConn(sock, heap).guarantee(release))
          }
          .foreverM
      }
    }

  private def ffmConn(sock: Tcp.Socket, heap: Boolean): IO[Unit] =
    if (!heap) {
      Resource.make(IO(Arena.ofShared()))(a => IO(a.close())).use { arena =>
        val buf = arena.allocate(BufSize.toLong, 64L)
        def loop: IO[Unit] =
          sock.read(buf, BufSize.toLong).flatMap { n =>
            if (n < 0) IO.unit else sock.writeAll(buf, n.toLong) >> loop
          }
        loop
      }
    } else {
      def loop: IO[Unit] =
        sock.readArray(BufSize).flatMap { chunk =>
          if (chunk eq null) IO.unit
          else sock.writeAll(MemorySegment.ofArray(chunk), chunk.length.toLong) >> loop
        }
      loop
    }

  // ---------------------------------------------------------------------------------------------
  // NIO via the stock SelectorSystem

  private[this] val nioScratchTL: ThreadLocal[ByteBuffer] =
    ThreadLocal.withInitial(() => ByteBuffer.allocateDirect(BufSize))

  private def nio(heap: Boolean): IO[Unit] =
    IO.pollers.map(_.collectFirst { case s: Selector => s }.get).flatMap { selector =>
      Supervisor[IO](await = false).use { sup =>
        Resource
          .make(IO {
            val ch = selector.provider.openServerSocketChannel()
            ch.configureBlocking(false)
            ch.setOption(StandardSocketOptions.SO_REUSEADDR, java.lang.Boolean.TRUE)
            ch.bind(new InetSocketAddress(port), 4096)
            ch
          })(ch => IO(ch.close()))
          .use { server =>
            def accept: IO[SocketChannel] =
              IO(server.accept()).flatMap { ch =>
                if (ch ne null) IO.pure(ch)
                else selector.select(server, SelectionKey.OP_ACCEPT) >> accept
              }
            accept
              .flatMap { ch =>
                IO {
                  ch.configureBlocking(false)
                  ch.setOption(StandardSocketOptions.TCP_NODELAY, java.lang.Boolean.TRUE)
                } >> sup.supervise(nioConn(selector, ch, heap).guarantee(IO(ch.close())))
              }
              .foreverM
          }
      }
    }

  private def nioConn(selector: Selector, ch: SocketChannel, heap: Boolean): IO[Unit] = {
    def writeAll(buf: ByteBuffer): IO[Unit] =
      IO { ch.write(buf); buf.hasRemaining }.flatMap { more =>
        if (more) selector.select(ch, SelectionKey.OP_WRITE) >> writeAll(buf) else IO.unit
      }

    def read(buf: ByteBuffer): IO[Int] =
      IO(ch.read(buf)).flatMap { n =>
        if (n == 0) selector.select(ch, SelectionKey.OP_READ) >> read(buf) else IO.pure(n)
      }

    if (!heap) {
      val buf = ByteBuffer.allocateDirect(BufSize)
      def loop: IO[Unit] =
        IO(buf.clear()) >> read(buf).flatMap { n =>
          if (n < 0) IO.unit else IO(buf.flip()) >> writeAll(buf) >> loop
        }
      loop
    } else {
      // read into this thread's scratch and copy out in the same step, like fs2
      def readArray: IO[Array[Byte]] =
        IO {
          val scratch = nioScratchTL.get()
          scratch.clear()
          val n = ch.read(scratch)
          if (n < 0) null
          else if (n == 0) Array.emptyByteArray
          else {
            val chunk = new Array[Byte](n)
            scratch.flip()
            scratch.get(chunk)
            chunk
          }
        }.flatMap { chunk =>
          if ((chunk ne null) && chunk.length == 0)
            selector.select(ch, SelectionKey.OP_READ) >> readArray
          else IO.pure(chunk)
        }
      def loop: IO[Unit] =
        readArray.flatMap { chunk =>
          if (chunk eq null) IO.unit else writeAll(ByteBuffer.wrap(chunk)) >> loop
        }
      loop
    }
  }
}
