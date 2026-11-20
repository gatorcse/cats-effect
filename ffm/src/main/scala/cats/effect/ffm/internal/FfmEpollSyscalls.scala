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

package cats.effect.ffm.internal

import cats.effect.unsafe.EpollSyscalls

import java.io.IOException
import java.lang.foreign.{Arena, MemorySegment, ValueLayout}

/**
 * [[EpollSyscalls]] through the Foreign Function & Memory API.
 *
 * All of a poller's native memory is one `Arena.ofShared()`, closed once in [[release]]. A
 * confined arena would not do: a worker hands its poller to a replacement thread on
 * `IO.blocking`.
 *
 * @param nanosecondTimeouts
 *   use `epoll_pwait2` for timed waits; otherwise milliseconds, rounded up so that a
 *   sub-millisecond deadline parks instead of spinning, and clamped to `Int.MaxValue`
 */
private[ffm] final class FfmEpollSyscalls(val maxEvents: Int, nanosecondTimeouts: Boolean)
    extends EpollSyscalls {

  import FfmEpollSyscalls._

  private[this] val arena: Arena = Arena.ofShared()
  private[this] val events: MemorySegment =
    arena.allocate(Epoll.EPOLL_EVENT_SIZE * maxEvents, 8L)
  private[this] val ctlEvent: MemorySegment = arena.allocate(Epoll.EPOLL_EVENT)
  private[this] val timespec: MemorySegment = arena.allocate(Epoll.TIMESPEC)
  private[this] val drainBuf: MemorySegment = arena.allocate(8L, 8L)

  /**
   * errno capture for the owner thread; other threads use their own.
   */
  private[this] val cap: MemorySegment = Libc.allocateCapture(arena)

  def epollCreate(): Int = {
    val fd = Epoll.epollCreate1(cap, Epoll.EPOLL_CLOEXEC)
    if (fd < 0) throw errnoException("epoll_create1", Libc.errno(cap))
    fd
  }

  def eventfdCreate(): Int = {
    val fd = Epoll.eventfd(cap, 0, Epoll.EFD_NONBLOCK | Epoll.EFD_CLOEXEC)
    if (fd < 0) throw errnoException("eventfd", Libc.errno(cap))
    fd
  }

  def epollAdd(epfd: Int, fd: Int, events: Int, data: Long): Int = {
    ctlEvent.set(ValueLayout.JAVA_INT, Epoll.EPOLL_EVENT_EVENTS_OFFSET, events)
    ctlEvent.set(Epoll.JAVA_LONG_UNALIGNED, Epoll.EPOLL_EVENT_DATA_OFFSET, data)
    if (Epoll.epollCtl(cap, epfd, Epoll.EPOLL_CTL_ADD, fd, ctlEvent) == 0) 0
    else Libc.errno(cap)
  }

  def epollDel(epfd: Int, fd: Int): Unit = {
    Epoll.epollCtl(Libc.capture(), epfd, Epoll.EPOLL_CTL_DEL, fd, MemorySegment.NULL)
    ()
  }

  def epollWait(epfd: Int, nanos: Long): Int = {
    val rtn =
      if (nanos == 0) Epoll.epollWaitNow(cap, epfd, events, maxEvents)
      else if (nanos < 0) Epoll.epollWait(cap, epfd, events, maxEvents, -1)
      else if (nanosecondTimeouts) {
        timespec.set(ValueLayout.JAVA_LONG, 0L, nanos / 1000000000L)
        timespec.set(ValueLayout.JAVA_LONG, 8L, nanos % 1000000000L)
        Epoll.epollPwait2(cap, epfd, events, maxEvents, timespec)
      } else {
        val millis = (nanos + 999999L) / 1000000L
        Epoll.epollWait(
          cap,
          epfd,
          events,
          maxEvents,
          Math.min(millis, Int.MaxValue.toLong).toInt)
      }
    if (rtn >= 0) rtn else -Libc.errno(cap)
  }

  def eventFlags(i: Int): Int =
    events.get(
      ValueLayout.JAVA_INT,
      i * Epoll.EPOLL_EVENT_SIZE + Epoll.EPOLL_EVENT_EVENTS_OFFSET)

  def eventData(i: Int): Long =
    events.get(
      Epoll.JAVA_LONG_UNALIGNED,
      i * Epoll.EPOLL_EVENT_SIZE + Epoll.EPOLL_EVENT_DATA_OFFSET)

  def eventfdSignal(fd: Int): Unit = {
    Libc.write(Libc.capture(), fd, One, 8L)
    ()
  }

  def eventfdDrain(fd: Int): Unit = {
    Libc.read(cap, fd, drainBuf, 8L)
    ()
  }

  def close(fd: Int): Unit = {
    Libc.close(cap, fd)
    ()
  }

  def release(): Unit = arena.close()
}

private[ffm] object FfmEpollSyscalls {

  /**
   * A constant 8-byte `1L` to write to an eventfd. Read-only, so shared.
   */
  private val One: MemorySegment = {
    val s = Arena.global().allocate(8L, 8L)
    s.set(ValueLayout.JAVA_LONG, 0L, 1L)
    s
  }

  private def errnoException(op: String, errno: Int): IOException =
    new IOException(s"$op failed: errno $errno")

  /**
   * Whether `epoll_pwait2` (Linux 5.11+, glibc 2.35+) actually works here. The symbol may exist
   * while a seccomp profile blocks the syscall, so this makes a real call, once.
   */
  lazy val epollPwait2Available: Boolean =
    Epoll.hasEpollPwait2Symbol() && {
      val arena = Arena.ofConfined()
      try {
        val cap = Libc.allocateCapture(arena)
        val epfd = Epoll.epollCreate1(cap, Epoll.EPOLL_CLOEXEC)
        if (epfd < 0) false
        else
          try {
            val events = arena.allocate(Epoll.EPOLL_EVENT)
            val ts = arena.allocate(Epoll.TIMESPEC) // zeroed: return immediately
            Epoll.epollPwait2(cap, epfd, events, 1, ts) >= 0
          } finally { Libc.close(cap, epfd); () }
      } catch {
        case _: Throwable => false
      } finally arena.close()
    }
}
