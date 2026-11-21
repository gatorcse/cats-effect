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
 * The Scala Native epoll polling system. The event loop is `EventLoop`, shared with the JVM's
 * `cats-effect-ffm` module; this object supplies the `@extern` syscalls.
 */
object EpollSystem extends EventLoopPollingSystem {

  private[this] final val MaxEvents = WorkStealingThreadPoolConstants.PollingTicks

  def makePoller(): Poller =
    new Poller(new NativeEpollSyscalls(MaxEvents, NativeEpollSyscalls.epollPwait2Available))

  // Re-declared with this object's concrete `Poller` so that the erased signatures (and the
  // static forwarders) stay binary compatible with the previous, standalone EpollSystem.

  override def close(): Unit = super.close()

  override def makeApi(ctx: PollingContext[Poller]): Api = super.makeApi(ctx)

  override def closePoller(poller: Poller): Unit = super.closePoller(poller)

  override def poll(poller: Poller, nanos: Long): PollResult = super.poll(poller, nanos)

  override def processReadyEvents(poller: Poller): Boolean = super.processReadyEvents(poller)

  override def needsPoll(poller: Poller): Boolean = super.needsPoll(poller)

  override def interrupt(targetThread: Thread, targetPoller: Poller): Unit =
    super.interrupt(targetThread, targetPoller)

  override def metrics(poller: Poller): cats.effect.unsafe.metrics.PollerMetrics =
    super.metrics(poller)

  final class Poller private[EpollSystem] (sys: EpollSyscalls)
      extends EventLoop.Poller(new EpollBackend(sys)) {
    override def toString: String = "Epoll" // as before the shared event loop
  }
}
