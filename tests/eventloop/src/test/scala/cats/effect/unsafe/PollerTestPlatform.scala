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
 * What the shared poller contract suites need from a platform: one event-loop backend to test
 * (epoll or kqueue), and a handful of raw, non-blocking fd operations. The JVM implements it
 * with FFM (`cats-effect-ffm`), Scala Native with `@extern`.
 */
trait PollerTestPlatform {

  /**
   * "epoll" or "kqueue", for test names.
   */
  def backendName: String

  /**
   * True where this backend can run here. Suites for other backends are skipped.
   */
  def backendSupported: Boolean

  /**
   * Whether timed waits can be in nanoseconds (`epoll_pwait2`; always for kqueue).
   */
  def nanosecondTimeoutsAvailable: Boolean

  def makeBackend(maxEvents: Int, nanosecondTimeouts: Boolean): EventLoopBackend

  /**
   * True when the backend is emulated (kqueue through libkqueue on Linux), which does not model
   * every socket state; the few cases that depend on one are skipped.
   */
  def emulated: Boolean = false

  /**
   * A polling system over this backend, for the `interrupt` contract.
   */
  def pollingSystem(): PollingSystem { type Poller <: EventLoop.Poller }

  /**
   * The polling system for the runtime contract; `None` uses the platform default.
   */
  def runtimePollingSystem: Option[PollingSystem]

  /**
   * A non-blocking, close-on-exec pipe as (readFd, writeFd).
   */
  def pipe(): (Int, Int)

  def close(fd: Int): Unit

  /**
   * Non-blocking read: `Left(())` on EAGAIN, `Right(-1)` at EOF. Throws on other errors.
   */
  def tryRead(fd: Int, buf: Array[Byte], off: Int, len: Int): Either[Unit, Int]

  /**
   * Non-blocking write: `Left(())` on EAGAIN. Throws on other errors (EPIPE included).
   */
  def tryWrite(fd: Int, buf: Array[Byte], off: Int, len: Int): Either[Unit, Int]

  /**
   * A non-blocking TCP socket that was never connected.
   */
  def unconnectedTcpSocket(): Int

  final def writeUnsafe(fd: Int, bytes: Array[Byte]): Unit =
    tryWrite(fd, bytes, 0, bytes.length).fold(_ => sys.error("pipe full"), _ => ())
}
