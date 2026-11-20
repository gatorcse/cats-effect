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

import cats.effect.std.CountDownLatch
import cats.syntax.all._

import scala.concurrent.duration._

import Fds._

/** Port of the Scala Native `FileDescriptorPollerSuite`, plus JVM-specific cases. */
class FileDescriptorPollerSuite extends FfmRuntimeSuite {

  real("installs FfmEpollSystem's FileDescriptorPoller") {
    FileDescriptorPoller.find.map(p => assert(p.isDefined))
  }

  real("notify read-ready events") {
    registeredPipe.use { pipe =>
      for {
        buf <- IO(new Array[Byte](4))
        _ <- pipe.write(Array[Byte](1, 2, 3), 0, 3).background.surround(pipe.read(buf, 0, 3))
        _ <- pipe.write(Array[Byte](42), 0, 1).background.surround(pipe.read(buf, 3, 1))
      } yield assertEquals(buf.toList, List[Byte](1, 2, 3, 42))
    }
  }

  real("handle lots of simultaneous events") {
    def test(n: Int) = registeredPipe.replicateA(n).use { pipes =>
      CountDownLatch[IO](n).flatMap { latch =>
        pipes
          .traverse_(pipe => (pipe.read(new Array[Byte](1), 0, 1) *> latch.release).background)
          .surround {
            IO(pipes.foreach(p => writeUnsafe(p.writeFd, Array[Byte](42))))
              .background
              .surround(latch.await)
          }
      }
    }
    // multiples of 64 exercise the Incomplete (more events than the array holds) path
    test(64) *> test(128) *> test(1000)
  }

  real("hang if never ready") {
    registeredPipe.use { pipe =>
      pipe
        .read(new Array[Byte](1), 0, 1)
        .map(_ => fail("shouldn't get there"))
        .timeoutTo(1.second, IO.unit)
    }
  }

  real("pollReadRec reads until EPOLLHUP then terminates with EOF") {
    pipe.use {
      case (r, w) =>
        FileDescriptorPoller.get.flatMap(_.registerFileDescriptor(r, true, false).use { h =>
          for {
            gate <- Deferred[IO, Unit]
            buf = new Array[Byte](1)
            reader = h.pollReadRec(()) { _ =>
              IO(tryRead(r, buf, 0, 1)).flatTap {
                case Left(_) => gate.complete(()).void
                case Right(_) => IO.unit
              }
            }
            n <- reader.background.use(join => gate.get >> IO(close(w)) >> join.flatMap(_.embedNever))
          } yield assertEquals(n, -1)
        })
    }
  }

  real("EPOLLERR wakes a blocked writer (reader closed)") {
    pipe.use {
      case (r, w) =>
        FileDescriptorPoller.get.flatMap(_.registerFileDescriptor(w, false, true).use { h =>
          val chunk = new Array[Byte](65536)
          // fill the pipe until EAGAIN
          IO { while (tryWrite(w, chunk, 0, chunk.length).isRight) () } >>
            Deferred[IO, Unit].flatMap { blocked =>
              val writer = h.pollWriteRec(()) { _ =>
                IO(tryWrite(w, chunk, 0, chunk.length)).flatTap {
                  case Left(_) => blocked.complete(()).void
                  case Right(_) => IO.unit
                }
              }
              writer.attempt.background.use { join =>
                blocked.get >> IO(close(r)) >> join.flatMap(_.embedNever)
              }
            }.map(res => assert(res.isLeft, s"expected EPIPE, got $res"))
        })
    }
  }

  real("concurrent writers on one handle are serialized") {
    registeredPipe.use { pipe =>
      val n = 200
      val writers = (0 until n).toList.parTraverse_ { i =>
        pipe.write(Array(i.toByte), 0, 1)
      }
      val readAll = {
        val buf = new Array[Byte](n)
        def go(off: Int): IO[Unit] =
          if (off == n) IO.unit else pipe.read(buf, off, n - off).flatMap(k => go(off + k))
        go(0).map(_ => buf.map(_ & 0xff).sorted.toList)
      }
      (writers, readAll).parMapN((_, bytes) => assertEquals(bytes, (0 until n).toList))
    }
  }

  real("cancel a waiting pollReadRec, then read again") {
    registeredPipe.use { pipe =>
      val buf = new Array[Byte](1)
      for {
        _ <- pipe.read(buf, 0, 1).timeoutTo(50.millis, IO.unit) // canceled while waiting
        _ <- IO(writeUnsafe(pipe.writeFd, Array[Byte](7)))
        n <- pipe.read(buf, 0, 1)
      } yield {
        assertEquals(n, 1)
        assertEquals(buf(0), 7.toByte)
      }
    }
  }

  real("cancel a fiber queued behind the read lock") {
    registeredPipe.use { pipe =>
      val buf = new Array[Byte](1)
      for {
        first <- pipe.read(buf, 0, 1).start // holds the lock, waiting for data
        _ <- IO.sleep(20.millis)
        second <- pipe.read(buf, 0, 1).start // queued behind the lock
        _ <- IO.sleep(20.millis)
        _ <- second.cancel
        _ <- IO(writeUnsafe(pipe.writeFd, Array[Byte](1)))
        _ <- first.joinWithNever
        _ <- IO(writeUnsafe(pipe.writeFd, Array[Byte](2)))
        n <- pipe.read(buf, 0, 1) // the lock must be free again
      } yield assertEquals((n, buf(0)), (1, 2.toByte))
    }
  }

  real("works across IO.blocking (poller handoff to a new thread)") {
    registeredPipe.use { pipe =>
      val buf = new Array[Byte](1)
      val roundTrip =
        pipe.write(Array[Byte](9), 0, 1).background.surround(pipe.read(buf, 0, 1))
      (roundTrip *> IO.blocking(Thread.sleep(5))).replicateA_(50) *> roundTrip
    }
  }

  real("sub-millisecond sleeps complete") {
    IO.monotonic.flatMap { start =>
      IO.sleep(300.micros).replicateA_(100) *> IO.monotonic.map { end =>
        assert(end - start >= 30.millis, s"100 x 300us sleeps took only ${end - start}")
      }
    }
  }
}
