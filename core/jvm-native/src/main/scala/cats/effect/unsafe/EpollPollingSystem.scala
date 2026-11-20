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
 * The `PollingSystem` half of the shared epoll backend. Platform modules supply only the
 * syscalls, by implementing `makePoller`: the `cats-effect-ffm` module's `FfmEpollSystem` on the
 * JVM, and `EpollSystem` on Scala Native.
 */
abstract class EpollPollingSystem private[effect] () extends PollingSystem {

  type Api = FileDescriptorPoller
  type Poller <: EpollEventLoop.Poller

  def close(): Unit = ()

  def makeApi(ctx: PollingContext[Poller]): Api = new EpollPollingSystem.Api[Poller](ctx)

  def closePoller(poller: Poller): Unit = poller.close()

  def poll(poller: Poller, nanos: Long): PollResult = poller.poll(nanos)

  def processReadyEvents(poller: Poller): Boolean = poller.processReadyEvents()

  def needsPoll(poller: Poller): Boolean = poller.needsPoll()

  /** The owner thread never wakes itself: it is not parked. */
  def interrupt(targetThread: Thread, targetPoller: Poller): Unit =
    if (Thread.currentThread() ne targetThread) targetPoller.interrupt()

  def metrics(poller: Poller): PollerMetrics = poller
}

private object EpollPollingSystem {

  private final class Api[P <: EpollEventLoop.Poller](ctx: PollingContext[P])
      extends FileDescriptorPoller {

    def registerFileDescriptor(
        fd: Int,
        reads: Boolean,
        writes: Boolean
    ): Resource[IO, FileDescriptorPollHandle] =
      Resource.make {
        IO.async_[EpollEventLoop.PollHandle] { cb =>
          ctx.accessPoller(poller => cb(poller.register(fd, reads, writes)))
        }
      }(handle => IO(handle.deregister()))
  }
}
