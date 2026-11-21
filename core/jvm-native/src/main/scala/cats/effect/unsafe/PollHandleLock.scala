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

import scala.annotation.tailrec

import java.util.concurrent.atomic.AtomicReference

/**
 * A minimal asynchronous, FIFO, cancelation-safe lock for one poll handle direction.
 *
 * Same contract as `std.Mutex` for our use, but the uncontended path is a single CAS with no
 * allocation beyond the bracketing `IO` nodes. Phase 0 measured `Mutex[IO]` at 15-25% of the
 * per-message cost of an echo server.
 */
private[effect] final class PollHandleLock {
  import PollHandleLock._

  private[this] val state = new AtomicReference[State](Free)

  def surround[A](body: IO[A]): IO[A] =
    IO.uncancelable { poll =>
      IO.defer {
        if (state.compareAndSet(Free, Held)) poll(body).guarantee(IO(release()))
        else poll(acquireSlow) *> poll(body).guarantee(IO(release()))
      }
    }

  private[this] def acquireSlow: IO[Unit] =
    IO.asyncCheckAttempt[Unit] { cb =>
      IO {
        val waiter = new Waiter(cb)
        if (enqueueOrAcquire(waiter)) Right(())
        else Left(Some(IO(cancel(waiter))))
      }
    }

  /**
   * Returns true if the lock was acquired directly; false if `w` is now queued.
   */
  @tailrec private[this] def enqueueOrAcquire(w: Waiter): Boolean = {
    val s = state.get()
    if (s eq Free) {
      if (state.compareAndSet(s, Held)) true else enqueueOrAcquire(w)
    } else {
      val next = new Waiting(s.waiters :+ w)
      if (state.compareAndSet(s, next)) false else enqueueOrAcquire(w)
    }
  }

  /**
   * A canceled waiter leaves the queue; if it had already been granted the lock, pass it on.
   */
  @tailrec private[this] def cancel(w: Waiter): Unit = {
    val s = state.get()
    val idx = s.waiters.indexWhere(_ eq w)
    if (idx < 0) {
      // not queued: release() already handed us the lock while we were being canceled
      release()
    } else {
      val rest = s.waiters.patch(idx, Nil, 1)
      val next = if (rest.isEmpty) Held else new Waiting(rest)
      if (!state.compareAndSet(s, next)) cancel(w)
    }
  }

  @tailrec private[this] def release(): Unit = {
    val s = state.get()
    if (s eq Held) {
      if (!state.compareAndSet(s, Free)) release()
    } else if (s eq Free) {
      throw new IllegalStateException("PollHandleLock released while free")
    } else {
      val head = s.waiters.head
      val rest = s.waiters.tail
      val next = if (rest.isEmpty) Held else new Waiting(rest)
      if (state.compareAndSet(s, next)) head.cb(RightUnit) // ownership passes to head
      else release()
    }
  }
}

private[effect] object PollHandleLock {
  private val RightUnit: Either[Throwable, Unit] = Right(())

  private final class Waiter(val cb: Either[Throwable, Unit] => Unit)

  private sealed abstract class State { def waiters: Vector[Waiter] }
  private object Free extends State { def waiters: Vector[Waiter] = Vector.empty }
  private object Held extends State { def waiters: Vector[Waiter] = Vector.empty }
  private final class Waiting(val waiters: Vector[Waiter]) extends State
}
