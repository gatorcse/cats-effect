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
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.string._
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

import java.io.IOException

/**
 * [[EpollSyscalls]] through Scala Native `@extern` calls.
 */
private[unsafe] final class NativeEpollSyscalls(val maxEvents: Int, nanosecondTimeouts: Boolean)
    extends EpollSyscalls {

  import NativeEpollSyscalls._

  private[this] val events: Ptr[Byte] =
    stdlib.calloc(maxEvents.toCSize, EventSize.toCSize).asInstanceOf[Ptr[Byte]]
  private[this] val timespec: Ptr[Long] =
    stdlib.calloc(2.toCSize, sizeof[Long]).asInstanceOf[Ptr[Long]]
  private[this] val drainBuf: Ptr[Long] =
    stdlib.calloc(1.toCSize, sizeof[Long]).asInstanceOf[Ptr[Long]]

  def epollCreate(): Int = {
    val fd = epoll.epoll_create1(EPOLL_CLOEXEC)
    if (fd < 0) throw errnoException("epoll_create1", errno)
    fd
  }

  def eventfdCreate(): Int = {
    val fd = eventfd.eventfd(0, eventfd.EFD_NONBLOCK | eventfd.EFD_CLOEXEC)
    if (fd < 0) throw errnoException("eventfd", errno)
    fd
  }

  def epollAdd(epfd: Int, fd: Int, events: Int, data: Long): Int = {
    val ev = stackalloc[Byte](16)
    !ev.asInstanceOf[Ptr[CInt]] = events
    !(ev + DataOffset).asInstanceOf[Ptr[Long]] = data
    if (epoll.epoll_ctl(epfd, EPOLL_CTL_ADD, fd, ev) == 0) 0 else errno
  }

  def epollDel(epfd: Int, fd: Int): Unit = {
    epoll.epoll_ctl(epfd, EPOLL_CTL_DEL, fd, null)
    ()
  }

  def epollWait(epfd: Int, nanos: Long): Int = {
    val rtn =
      if (nanos == 0) epoll.immediate.epoll_wait(epfd, events, maxEvents, 0)
      else if (nanos < 0) epoll.awaiting.epoll_wait(epfd, events, maxEvents, -1)
      else if (nanosecondTimeouts) {
        timespec(0) = nanos / 1000000000L
        timespec(1) = nanos % 1000000000L
        epoll
          .awaiting
          .syscall(SYS_epoll_pwait2, epfd.toLong, events, maxEvents.toLong, timespec, null, 0L)
          .toInt
      } else {
        // round up, without overflowing near Long.MaxValue (a negative timeout waits forever)
        val millis = nanos / 1000000L + (if (nanos % 1000000L != 0L) 1L else 0L)
        epoll
          .awaiting
          .epoll_wait(epfd, events, maxEvents, Math.min(millis, Int.MaxValue.toLong).toInt)
      }
    if (rtn >= 0) rtn else -errno
  }

  def eventFlags(i: Int): Int =
    !(events + i.toLong * EventSize).asInstanceOf[Ptr[CInt]]

  def eventData(i: Int): Long =
    !(events + (i.toLong * EventSize + DataOffset)).asInstanceOf[Ptr[Long]]

  def eventfdSignal(fd: Int): Unit = {
    val buf = stackalloc[Long]()
    !buf = 1L
    unistd.write(fd, buf, 8.toCSize)
    ()
  }

  def eventfdDrain(fd: Int): Unit = {
    unistd.read(fd, drainBuf, 8.toCSize)
    ()
  }

  def close(fd: Int): Unit = {
    unistd.close(fd)
    ()
  }

  def release(): Unit = {
    stdlib.free(events)
    stdlib.free(timespec.asInstanceOf[Ptr[Byte]])
    stdlib.free(drainBuf.asInstanceOf[Ptr[Byte]])
  }
}

private[unsafe] object NativeEpollSyscalls {

  final val EPOLL_CLOEXEC = 0x80000
  final val EPOLL_CTL_ADD = 1
  final val EPOLL_CTL_DEL = 2

  /**
   * `epoll_pwait2` has the same number on every architecture (new syscalls are unified).
   */
  final val SYS_epoll_pwait2 = 441L

  /**
   * `struct epoll_event` is packed (12 bytes) on x86_64 only.
   */
  val EventSize: Long = if (LinktimeInfo.target.arch == "x86_64") 12L else 16L
  val DataOffset: Long = if (LinktimeInfo.target.arch == "x86_64") 4L else 8L

  private def errnoException(op: String, errno: Int): IOException =
    new IOException(s"$op failed: ${fromCString(strerror(errno))}")

  /**
   * Whether `epoll_pwait2` (Linux 5.11+) works here. Called through `syscall(2)` so that it
   * does not depend on the libc version; a seccomp profile may still block it, so make a real
   * call.
   */
  lazy val epollPwait2Available: Boolean = {
    val epfd = epoll.epoll_create1(EPOLL_CLOEXEC)
    if (epfd < 0) false
    else
      try {
        val ev = stackalloc[Byte](16)
        val ts = stackalloc[Long](2) // zeroed: return immediately
        epoll.immediate.syscall(SYS_epoll_pwait2, epfd.toLong, ev, 1L, ts, null, 0L) >= 0
      } finally { unistd.close(epfd); () }
  }

  @nowarn212
  @extern
  private[unsafe] object epoll {

    def epoll_create1(flags: Int): Int = extern

    def epoll_ctl(epfd: Int, op: Int, fd: Int, event: Ptr[Byte]): Int = extern

    @extern
    object awaiting {
      @blocking
      def epoll_wait(epfd: Int, events: Ptr[Byte], maxevents: Int, timeout: Int): Int = extern

      /**
       * `syscall(2)` with a fixed prototype; all arguments are integers or pointers.
       */
      @blocking
      def syscall(
          number: Long,
          a1: Long,
          a2: Ptr[Byte],
          a3: Long,
          a4: Ptr[Long],
          a5: Ptr[Byte],
          a6: Long): Long = extern
    }

    @extern
    object immediate {
      def epoll_wait(epfd: Int, events: Ptr[Byte], maxevents: Int, timeout: Int): Int = extern

      def syscall(
          number: Long,
          a1: Long,
          a2: Ptr[Byte],
          a3: Long,
          a4: Ptr[Long],
          a5: Ptr[Byte],
          a6: Long): Long = extern
    }
  }

  @nowarn212
  @extern
  private[unsafe] object eventfd {
    final val EFD_CLOEXEC = 0x80000
    final val EFD_NONBLOCK = 0x00800

    def eventfd(initval: Int, flags: Int): Int = extern
  }
}
