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
 * The kqueue calls the [[KqueueBackend]] needs, implemented with FFM on the JVM and `@extern`
 * on Scala Native. Uses `struct kevent` (not `kevent64`), which is 32 bytes on 64-bit macOS and
 * in libkqueue. An instance owns one native event list and is used by one poller.
 */
private[effect] trait KqueueSyscalls {

  def maxEvents: Int

  /**
   * `EVFILT_USER` on this OS: -10 on macOS, -11 on FreeBSD and in libkqueue.
   */
  def userFilter: Short

  /**
   * `kqueue()`; throws `IOException` on failure.
   */
  def kqueueCreate(): Int

  /**
   * Applies one change with `kevent(kq, &change, 1, NULL, 0, &zero)`. Returns 0 or the errno.
   * Any thread.
   */
  def keventChange(
      kq: Int,
      ident: Long,
      filter: Short,
      flags: Int,
      fflags: Int,
      udata: Long): Int

  /**
   * Waits for up to [[maxEvents]] events into this instance's event list. `nanos` is -1
   * (forever), 0 (don't block) or a positive timeout, which is clamped to
   * [[KqueueConstants.MaxTimeoutSeconds]]. Returns the event count, or `-errno`.
   */
  def keventWait(kq: Int, nanos: Long): Int

  def eventFilter(i: Int): Short
  def eventFlags(i: Int): Int
  def eventData(i: Int): Long
  def eventUdata(i: Int): Long

  def close(fd: Int): Unit

  /**
   * Frees this instance's native memory. Called once, last.
   */
  def release(): Unit
}

private[effect] object KqueueConstants {
  final val EVFILT_READ: Short = -1
  final val EVFILT_WRITE: Short = -2

  final val EV_ADD = 0x0001
  final val EV_DELETE = 0x0002
  final val EV_ONESHOT = 0x0010
  final val EV_CLEAR = 0x0020
  final val EV_ERROR = 0x4000
  final val EV_EOF = 0x8000

  final val NOTE_TRIGGER = 0x01000000

  /**
   * `kevent` rejects larger timeouts with `EINVAL` (Netty #13503); the worker recomputes its
   * deadline after a timeout, so clamping is invisible.
   */
  final val MaxTimeoutSeconds = 86399L

  final val sizeofKevent = 32
}
