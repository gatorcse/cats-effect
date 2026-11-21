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

import org.typelevel.scalaccompat.annotation._

import scala.scalanative.libc.errno._
import scala.scalanative.libc.stdlib
import scala.scalanative.posix.string._
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

import java.io.IOException

import KqueueConstants._

/**
 * `@extern` kqueue calls for the shared [[KqueueBackend]] (macOS).
 */
private[unsafe] final class NativeKqueueSyscalls(val maxEvents: Int) extends KqueueSyscalls {

  import NativeKqueueSyscalls._

  private[this] val events: Ptr[Byte] =
    stdlib.calloc(maxEvents.toCSize, sizeofKevent.toCSize).asInstanceOf[Ptr[Byte]]
  private[this] val timespec: Ptr[Long] =
    stdlib.calloc(2.toCSize, sizeof[Long]).asInstanceOf[Ptr[Long]]

  def userFilter: Short = EVFILT_USER_MACOS

  def kqueueCreate(): Int = {
    val fd = kq.kqueue()
    if (fd < 0) throw new IOException(s"kqueue failed: ${fromCString(strerror(errno))}")
    fd
  }

  def keventChange(
      kqfd: Int,
      ident: Long,
      filter: Short,
      flags: Int,
      fflags: Int,
      udata: Long): Int = {
    val ev = stackalloc[Byte](sizeofKevent)
    !ev.asInstanceOf[Ptr[Long]] = ident
    !(ev + 8).asInstanceOf[Ptr[Short]] = filter
    !(ev + 10).asInstanceOf[Ptr[Short]] = flags.toShort
    !(ev + 12).asInstanceOf[Ptr[CInt]] = fflags
    !(ev + 16).asInstanceOf[Ptr[Long]] = 0L
    !(ev + 24).asInstanceOf[Ptr[Long]] = udata
    val zero = stackalloc[Long](2)
    if (kq.immediate.kevent(kqfd, ev, 1, null, 0, zero) >= 0) 0 else errno
  }

  def keventWait(kqfd: Int, nanos: Long): Int = {
    val rtn =
      if (nanos == 0) {
        timespec(0) = 0L
        timespec(1) = 0L
        kq.immediate.kevent(kqfd, null, 0, events, maxEvents, timespec)
      } else if (nanos < 0) kq.awaiting.kevent(kqfd, null, 0, events, maxEvents, null)
      else {
        val secs = nanos / 1000000000L
        if (secs >= MaxTimeoutSeconds) {
          timespec(0) = MaxTimeoutSeconds
          timespec(1) = 0L
        } else {
          timespec(0) = secs
          timespec(1) = nanos % 1000000000L
        }
        kq.awaiting.kevent(kqfd, null, 0, events, maxEvents, timespec)
      }
    if (rtn >= 0) rtn else -errno
  }

  @inline private[this] def event(i: Int): Ptr[Byte] = events + i.toLong * sizeofKevent

  def eventFilter(i: Int): Short = !(event(i) + 8).asInstanceOf[Ptr[Short]]
  def eventFlags(i: Int): Int = (!(event(i) + 10).asInstanceOf[Ptr[Short]]).toInt & 0xffff
  def eventData(i: Int): Long = !(event(i) + 16).asInstanceOf[Ptr[Long]]
  def eventUdata(i: Int): Long = !(event(i) + 24).asInstanceOf[Ptr[Long]]

  def close(fd: Int): Unit = {
    unistd.close(fd)
    ()
  }

  def release(): Unit = {
    stdlib.free(events)
    stdlib.free(timespec.asInstanceOf[Ptr[Byte]])
  }
}

private[unsafe] object NativeKqueueSyscalls {

  final val EVFILT_USER_MACOS: Short = -10

  @nowarn212
  @extern
  private[unsafe] object kq {

    def kqueue(): CInt = extern

    @extern
    object awaiting {
      @blocking
      def kevent(
          kq: CInt,
          changelist: Ptr[Byte],
          nchanges: CInt,
          eventlist: Ptr[Byte],
          nevents: CInt,
          timeout: Ptr[Long]): CInt = extern
    }

    @extern
    object immediate {
      def kevent(
          kq: CInt,
          changelist: Ptr[Byte],
          nchanges: CInt,
          eventlist: Ptr[Byte],
          nevents: CInt,
          timeout: Ptr[Long]): CInt = extern
    }
  }
}
