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

import cats.effect.ffm.internal.FfmKqueueSyscalls
import cats.effect.unsafe.{EventLoop, EventLoopPollingSystem, KqueueBackend}

/**
 * A [[cats.effect.unsafe.PollingSystem PollingSystem]] for macOS backed by `kqueue`, called
 * directly through the Foreign Function & Memory API (JDK 25+). Its `Api` is the same
 * [[cats.effect.FileDescriptorPoller FileDescriptorPoller]] as [[FfmEpollSystem]]'s and the Scala
 * Native runtime's.
 *
 * Usually obtained through [[FfmPollingSystem]]. It shares the event loop with
 * [[FfmEpollSystem]] (see there for the design); the kqueue specifics are:
 *
 *   - Each handle registers `EV_CLEAR` (edge-triggered) read and/or write filters once, with
 *     the generation-tagged handle id in `udata`.
 *   - Wake-ups trigger an `EV_CLEAR` `EVFILT_USER` event, debounced like epoll's eventfd.
 *   - Timeouts are a `timespec` in nanoseconds, clamped to 86,399 s (`kevent` rejects larger
 *     ones).
 *   - `EV_ERROR` wakes both readers and writers; `EV_EOF` wakes the filter it arrives on.
 */
final class FfmKqueueSystem private (maxEvents: Int) extends EventLoopPollingSystem {

  type Poller = EventLoop.Poller

  def makePoller(): Poller =
    new EventLoop.Poller(new KqueueBackend(new FfmKqueueSyscalls(maxEvents)))

  override def toString: String = s"FfmKqueueSystem(maxEvents = $maxEvents)"
}

object FfmKqueueSystem {

  final val DefaultMaxEvents = 64

  /**
   * Creates a kqueue polling system. Throws if kqueue is unavailable; prefer
   * [[FfmPollingSystem.apply]], which checks first and falls back.
   *
   * @param maxEvents
   *   the size of each worker's ready-event array, i.e. the most events handled per poll
   */
  def apply(maxEvents: Int = DefaultMaxEvents): FfmKqueueSystem = {
    require(maxEvents > 0, s"maxEvents must be positive: $maxEvents")
    new FfmKqueueSystem(maxEvents)
  }
}
