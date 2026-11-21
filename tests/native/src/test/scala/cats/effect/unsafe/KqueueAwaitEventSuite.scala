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
package unsafe

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.scalanative.meta.LinktimeInfo

import munit.FunSuite

import KqueueConstants.{EV_ADD, EVFILT_READ}

/**
 * `KqueueSystem.Kqueue.awaitEvent`, the one-shot API kept for compatibility (macOS only).
 */
class KqueueAwaitEventSuite
    extends FunSuite
    with NativePollerTestPlatform
    with NativeKqueueTestBackend {

  override def munitIgnore: Boolean = !LinktimeInfo.isMac

  private[this] var _runtime: IORuntime = _
  implicit def runtime: IORuntime = _runtime

  // one worker, so that every step of a test reaches the same poller (claims are per poller)
  override def beforeAll(): Unit =
    if (LinktimeInfo.isMac) {
      val (compute, api, shutdown) =
        IORuntime.createWorkStealingComputeThreadPool(threads = 1, pollingSystem = KqueueSystem)
      _runtime =
        IORuntime.builder().setCompute(compute, shutdown).addPoller(api, () => ()).build()
    }

  override def afterAll(): Unit = if (_runtime ne null) _runtime.shutdown()

  def real(name: String)(body: => IO[Any]): Unit =
    test(name)(IO.defer(body).timeout(20.seconds).unsafeToFuture(): Future[Any])

  private def kqueue: IO[KqueueSystem.Kqueue] =
    IO.pollers.map(_.collectFirst { case k: KqueueSystem.Kqueue => k }.get)

  private def withPipe[A](f: (Int, Int) => IO[A]): IO[A] =
    IO(pipe()).bracket { case (r, w) => f(r, w) } { case (r, w) => IO { close(r); close(w) } }

  private def await(kq: KqueueSystem.Kqueue, fd: Int): IO[Long] =
    kq.awaitEvent(fd, EVFILT_READ, EV_ADD.toShort, 0)

  real("returns the event's data once it fires, waking a parked worker") {
    withPipe { (r, w) =>
      for {
        kq <- kqueue
        fiber <- await(kq, r).start
        _ <- IO.sleep(100.millis) // let the worker park with only the one-shot pending
        _ <- IO(writeUnsafe(w, Array[Byte](1, 2, 3)))
        n <- fiber.joinWithNever
      } yield assertEquals(n, 3L)
    }
  }

  real("a canceled wait is removed, so the pair can be waited on again") {
    withPipe { (r, w) =>
      for {
        kq <- kqueue
        _ <- await(kq, r).timeoutTo(50.millis, IO.pure(-1L))
        _ <- IO(writeUnsafe(w, Array[Byte](1)))
        n <- await(kq, r)
      } yield assertEquals(n, 1L)
    }
  }

  real("a bad descriptor fails") {
    kqueue.flatMap(await(_, -1).attempt).map(r => assert(r.isLeft, r))
  }

  real("a descriptor registered as a handle is not taken over") {
    withPipe { (r, w) =>
      kqueue.flatMap { kq =>
        kq.registerFileDescriptor(r, reads = true, writes = false).use { h =>
          for {
            clash <- await(kq, r).attempt
            _ <- IO(assert(clash.isLeft, clash))
            _ <- IO(writeUnsafe(w, Array[Byte](7)))
            buf = new Array[Byte](1)
            n <- h.pollReadRec(())(_ => IO(tryRead(r, buf, 0, 1)))
          } yield assertEquals(n, 1)
        }
      }
    }
  }
}
