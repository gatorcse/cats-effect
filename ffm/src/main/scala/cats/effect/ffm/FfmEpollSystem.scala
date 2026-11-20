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

import cats.effect.ffm.internal.{EpollEventLoop, FfmEpollSyscalls}
import cats.effect.unsafe.{PollResult, PollingContext, PollingSystem}
import cats.effect.unsafe.metrics.PollerMetrics


/**
 * A [[cats.effect.unsafe.PollingSystem PollingSystem]] for Linux backed by `epoll`, called
 * directly through the Foreign Function & Memory API (JDK 22+). Its `Api` is the same
 * [[cats.effect.FileDescriptorPoller FileDescriptorPoller]] that the Scala Native runtime
 * provides, so fd-based code (e.g. fs2 sockets) can be shared between the two platforms.
 *
 * Usually obtained through [[FfmPollingSystem]], which falls back to `SelectorSystem` when FFM
 * or epoll is unavailable.
 *
 * Design, relative to the Scala Native `EpollSystem` it is ported from:
 *
 *   - Registrations are edge-triggered. Readiness is published to each handle as a counter, so
 *     a fiber that races a notification never misses it.
 *   - `epoll_data` holds a generation-tagged handle id (slot, generation), not a pointer. An
 *     event for a registration that was removed, or whose slot was reused, is dropped.
 *   - Wake-ups go through an `eventfd`, debounced so that only the first interrupt per park
 *     writes, and never from the owner thread. Interrupts and deregistrations that race
 *     `closePoller` never touch a closed (and possibly reused) file descriptor.
 *   - Timeouts are in nanoseconds via `epoll_pwait2` when the kernel allows it, otherwise
 *     milliseconds rounded up, so a sub-millisecond timer parks instead of spinning.
 *   - `EPOLLHUP`, `EPOLLERR` and `EPOLLRDHUP` wake both readers and writers.
 *   - Each handle direction is serialized by a lightweight lock instead of `std.Mutex`.
 */
final class FfmEpollSystem private (maxEvents: Int, nanosecondTimeouts: Boolean)
    extends PollingSystem {

  type Api = FileDescriptorPoller
  type Poller = EpollEventLoop.Poller

  def close(): Unit = ()

  def makeApi(ctx: PollingContext[Poller]): Api = new FileDescriptorPollerImpl(ctx)

  def makePoller(): Poller =
    new EpollEventLoop.Poller(new FfmEpollSyscalls(maxEvents, nanosecondTimeouts))

  def closePoller(poller: Poller): Unit = poller.close()

  def poll(poller: Poller, nanos: Long): PollResult = poller.poll(nanos)

  def processReadyEvents(poller: Poller): Boolean = poller.processReadyEvents()

  def needsPoll(poller: Poller): Boolean = poller.needsPoll()

  def interrupt(targetThread: Thread, targetPoller: Poller): Unit =
    if (Thread.currentThread() ne targetThread) targetPoller.interrupt()

  def metrics(poller: Poller): PollerMetrics = poller

  override def toString: String =
    s"FfmEpollSystem(maxEvents = $maxEvents, nanosecondTimeouts = $nanosecondTimeouts)"

  private final class FileDescriptorPollerImpl(ctx: PollingContext[Poller])
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

object FfmEpollSystem {

  final val DefaultMaxEvents = 64

  /**
   * Creates an epoll polling system. Throws if epoll is unavailable; prefer
   * [[FfmPollingSystem.apply]], which checks first and falls back.
   *
   * @param maxEvents
   *   the size of each worker's ready-event array, i.e. the most events handled per poll
   * @param nanosecondTimeouts
   *   use `epoll_pwait2` for timed waits when the kernel supports it
   */
  def apply(
      maxEvents: Int = DefaultMaxEvents,
      nanosecondTimeouts: Boolean = true): FfmEpollSystem = {
    require(maxEvents > 0, s"maxEvents must be positive: $maxEvents")
    new FfmEpollSystem(maxEvents, nanosecondTimeouts && epollPwait2Available)
  }

  /**
   * Whether `epoll_pwait2` (Linux 5.11+, glibc 2.35+) actually works here. The symbol may exist
   * while a seccomp profile blocks the syscall, so this makes a real call, once.
   */
  def epollPwait2Available: Boolean = FfmEpollSyscalls.epollPwait2Available

}
