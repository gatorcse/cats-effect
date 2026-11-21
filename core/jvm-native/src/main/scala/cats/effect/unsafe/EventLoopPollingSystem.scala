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

import cats.effect.unsafe.metrics.PollerMetrics

/**
 * The `PollingSystem` half of the shared [[EventLoop]]. Platform modules supply only the
 * backend, by implementing `makePoller`: the `cats-effect-ffm` module's `FfmEpollSystem` and
 * `FfmKqueueSystem` on the JVM, and `EpollSystem` on Scala Native.
 */
abstract class EventLoopPollingSystem private[effect] () extends PollingSystem {

  type Api = FileDescriptorPoller
  type Poller <: EventLoop.Poller

  def close(): Unit = ()

  def makeApi(ctx: PollingContext[Poller]): Api = new EventLoopPollingSystem.Api[Poller](ctx)

  def closePoller(poller: Poller): Unit = poller.close()

  def poll(poller: Poller, nanos: Long): PollResult = poller.poll(nanos)

  def processReadyEvents(poller: Poller): Boolean = poller.processReadyEvents()

  def needsPoll(poller: Poller): Boolean = poller.needsPoll()

  /**
   * Always wakes the poller, even when called from `targetThread`. A worker that is about to
   * park first flags itself `ParkedPolling` and then calls `notifyIfWorkPending`, whose
   * `notifyParked` scans every index, its own included. If it picks itself, it marks itself
   * awake and interrupts its own poller, then goes on to `poll(-1)`. Skipping that "self"
   * wake-up left it parked forever with nothing to wake it (its signal is already `Unparked`).
   * The wake-up (an eventfd write, or an `EVFILT_USER` trigger on kqueue) stays pending, so the
   * next poll returns at once with `Complete` and the worker takes the "polled" exit, which
   * keeps the pool's counters balanced; it is debounced, so a redundant one is cheap.
   * (`LockSupport.unpark` and NIO's `Selector.wakeup` are sticky the same way.)
   */
  def interrupt(targetThread: Thread, targetPoller: Poller): Unit =
    targetPoller.interrupt()

  def metrics(poller: Poller): PollerMetrics = poller
}

private[unsafe] object EventLoopPollingSystem {

  private[unsafe] final class Api[P <: EventLoop.Poller](ctx: PollingContext[P])
      extends FileDescriptorPoller {

    def registerFileDescriptor(
        fd: Int,
        reads: Boolean,
        writes: Boolean
    ): Resource[IO, FileDescriptorPollHandle] =
      Resource.make {
        IO.async_[EventLoop.PollHandle] { cb =>
          ctx.accessPoller(poller => cb(poller.register(fd, reads, writes)))
        }
      }(handle => IO(handle.deregister()))
  }
}
