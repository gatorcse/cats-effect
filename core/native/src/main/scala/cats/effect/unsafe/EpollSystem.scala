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

/**
 * The Scala Native epoll polling system. The event loop is `EpollEventLoop`, shared with the
 * JVM's `cats-effect-ffm` module; this object supplies the `@extern` syscalls.
 */
object EpollSystem extends EpollPollingSystem {

  private[this] final val MaxEvents = WorkStealingThreadPoolConstants.PollingTicks

  def makePoller(): Poller =
    new Poller(new NativeEpollSyscalls(MaxEvents, NativeEpollSyscalls.epollPwait2Available))

  final class Poller private[EpollSystem] (sys: EpollSyscalls)
      extends EpollEventLoop.Poller(sys)
}
