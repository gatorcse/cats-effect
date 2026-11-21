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
 * The Scala Native kqueue polling system (macOS). The event loop is `EventLoop`, shared with
 * epoll and with the JVM's `cats-effect-ffm` module; this object supplies the `@extern`
 * syscalls. Handles are edge-triggered (`EV_CLEAR`) and registered once, rather than one
 * one-shot filter per wait.
 */
object KqueueSystem extends PollingSystem {

  private[this] final val MaxEvents = WorkStealingThreadPoolConstants.PollingTicks

  type Api = Kqueue

  def close(): Unit = ()

  def makeApi(ctx: PollingContext[Poller]): Kqueue = new Kqueue(ctx)

  def makePoller(): Poller = new Poller(new KqueueBackend(new NativeKqueueSyscalls(MaxEvents)))

  def closePoller(poller: Poller): Unit = poller.close()

  def poll(poller: Poller, nanos: Long): PollResult = poller.poll(nanos)

  def processReadyEvents(poller: Poller): Boolean = poller.processReadyEvents()

  def needsPoll(poller: Poller): Boolean = poller.needsPoll()

  /**
   * Always wakes the poller; see `EventLoopPollingSystem.interrupt`.
   */
  def interrupt(targetThread: Thread, targetPoller: Poller): Unit = targetPoller.interrupt()

  def metrics(poller: Poller): PollerMetrics = poller

  final class Kqueue private[KqueueSystem] (
      ctx: PollingContext[Poller]
  ) extends FileDescriptorPoller {

    private[this] val delegate = new EventLoopPollingSystem.Api[Poller](ctx)

    def registerFileDescriptor(
        fd: Int,
        reads: Boolean,
        writes: Boolean
    ): Resource[IO, FileDescriptorPollHandle] =
      delegate.registerFileDescriptor(fd, reads, writes)

    /**
     * Waits for one occurrence of the `(ident, filter)` event, registered as a one-shot filter
     * with the given `flags` and `fflags`, and returns its `data`. For filters other than reads
     * and writes on a descriptor, which `registerFileDescriptor` covers. Fails if a registered
     * handle or another pending `awaitEvent` already uses the same `(ident, filter)`.
     */
    def awaitEvent(
        ident: Int,
        filter: Short,
        flags: Short,
        fflags: Int
    ): IO[Long] =
      IO.async[Long] { cb =>
        IO.async_[Option[IO[Unit]]] { cancelCb =>
          ctx.accessPoller { poller =>
            // counted in the read/write metrics, as before the shared event loop
            val (submitted, succeeded, errored, canceled) =
              if (filter == KqueueConstants.EVFILT_READ)
                (
                  poller.readSubmitted,
                  poller.readSucceeded,
                  poller.readErrored,
                  poller.readCanceled)
              else if (filter == KqueueConstants.EVFILT_WRITE)
                (
                  poller.writeSubmitted,
                  poller.writeSucceeded,
                  poller.writeErrored,
                  poller.writeCanceled)
              else (null, null, null, null)
            if (submitted ne null) { submitted.incrementAndGet(); () }
            val counted: Either[Throwable, Long] => Unit = r => {
              if (submitted ne null) {
                (if (r.isRight) succeeded else errored).incrementAndGet()
                ()
              }
              cb(r)
            }
            val id =
              poller.backend.addOneShot(ident, filter, flags.toInt & 0xffff, fflags, counted)
            cancelCb(Right(Some(IO {
              if (poller.backend.cancelOneShot(id) && (canceled ne null)) {
                canceled.incrementAndGet()
                ()
              }
            })))
          }
        }
      }
  }

  final class Poller private[KqueueSystem] (private[KqueueSystem] val backend: KqueueBackend)
      extends EventLoop.Poller(backend)
}
