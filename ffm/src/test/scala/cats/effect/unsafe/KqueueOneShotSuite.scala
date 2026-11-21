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

import cats.effect.ffm.{FfmKqueueTestBackend, FfmPollerTestPlatform}

import java.util.concurrent.atomic.AtomicReference

import munit.FunSuite

import KqueueConstants.EVFILT_READ

/**
 * The one-shot path behind Native `KqueueSystem.awaitEvent`, driven directly on the shared
 * `KqueueBackend` (macOS, or libkqueue on Linux).
 */
class KqueueOneShotSuite extends FunSuite with FfmPollerTestPlatform with FfmKqueueTestBackend {

  override def munitIgnore: Boolean = !backendSupported

  private def withPipeAndPoller(f: (KqueueBackend, EventLoop.Poller, Int, Int) => Unit): Unit = {
    val backend = makeBackend(64, nanosecondTimeouts = true).asInstanceOf[KqueueBackend]
    val p = new EventLoop.Poller(backend)
    val (r, w) = pipe()
    try f(backend, p, r, w)
    finally { p.close(); close(r); close(w) }
  }

  private def drain(p: EventLoop.Poller): Unit = {
    if (p.poll(0) ne PollResult.Interrupted) { p.processReadyEvents(); () }
  }

  test("fires once with the event's data, then frees the pair") {
    withPipeAndPoller { (b, p, r, w) =>
      val result = new AtomicReference[Either[Throwable, Long]]
      b.addOneShot(r, EVFILT_READ, 0, 0, result.set(_))
      assert(p.needsPoll(), "a pending one-shot must keep the owner polling")
      writeUnsafe(w, Array[Byte](1, 2))
      drain(p)
      assertEquals(result.get(), Right(2L))
      assert(!p.needsPoll())
      // the pair is free again
      assert(p.register(r, reads = true, writes = false).isRight)
    }
  }

  test("a canceled one-shot is deleted: it never fires, and the pair is free") {
    withPipeAndPoller { (b, p, r, w) =>
      val result = new AtomicReference[Either[Throwable, Long]]
      val id = b.addOneShot(r, EVFILT_READ, 0, 0, result.set(_))
      assert(b.cancelOneShot(id))
      assert(!b.cancelOneShot(id), "canceled twice")
      writeUnsafe(w, Array[Byte](1))
      // nothing at all is reported: the kernel entry is gone, not just ignored
      assertEquals(p.poll(0), PollResult.Interrupted, "the canceled one-shot's entry lingered")
      assertEquals(result.get(), null)
      // and a handle on the pair gets a plain EV_CLEAR filter: an edge per write, not one shot
      val h = p.register(r, reads = true, writes = false).fold(throw _, identity)
      drain(p)
      val first = h.readReadyCount
      assertEquals(first, 1)
      writeUnsafe(w, Array[Byte](2))
      drain(p)
      assertEquals(h.readReadyCount, first + 1)
    }
  }

  test("a pair in use by a handle, the wake-up or another one-shot is refused") {
    withPipeAndPoller { (b, p, r, _) =>
      p.register(r, reads = true, writes = false).fold(throw _, identity)
      val clash = new AtomicReference[Either[Throwable, Long]]
      b.addOneShot(r, EVFILT_READ, 0, 0, clash.set(_))
      assert(clash.get().isLeft, clash.get())

      val user = new AtomicReference[Either[Throwable, Long]]
      b.addOneShot(0, userFilterForTests, 0, 0, user.set(_))
      assert(user.get().isLeft, "the wake-up's EVFILT_USER pair was handed out")
      // and the wake-up still works
      p.interrupt()
      assertEquals(p.poll(0), PollResult.Complete)
      p.processReadyEvents(): Unit
    }
  }

  test("a bad descriptor fails without keeping the pair") {
    withPipeAndPoller { (b, _, _, _) =>
      val result = new AtomicReference[Either[Throwable, Long]]
      b.addOneShot(-1, EVFILT_READ, 0, 0, result.set(_))
      assert(result.get().isLeft, result.get())
      assert(!b.hasOther)
    }
  }

  private def userFilterForTests: Short =
    if (cats.effect.ffm.internal.Libc.IS_MAC &&
      System.getProperty("cats.effect.ffm.kqueueLibrary") == null) -10
    else -11
}
