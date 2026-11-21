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

import cats.effect.unsafe.KqueueSyscalls

import java.io.IOException
import java.lang.foreign.{Arena, MemorySegment, ValueLayout}

import Kqueue._

/**
 * [[KqueueSyscalls]] through the Foreign Function & Memory API.
 *
 * As for epoll, the event list and timeout are one `Arena.ofShared()` per poller. Changes may
 * come from any thread (deregistration, wake-ups), so they use a per-thread scratch segment.
 */
private[ffm] final class FfmKqueueSyscalls(val maxEvents: Int) extends KqueueSyscalls {

  import FfmKqueueSyscalls._

  private[this] val arena: Arena = Arena.ofShared()
  private[this] val events: MemorySegment = arena.allocate(KEVENT_SIZE * maxEvents, 8L)
  private[this] val timespec: MemorySegment = arena.allocate(16L, 8L)
  private[this] val zero: MemorySegment = arena.allocate(16L, 8L)
  private[this] val cap: MemorySegment = Libc.allocateCapture(arena)

  def userFilter: Short = EVFILT_USER

  def kqueueCreate(): Int = {
    val fd = Kqueue.kqueue(cap)
    if (fd < 0) throw new IOException(s"kqueue failed: errno ${Libc.errno(cap)}")
    // kqueue descriptors are not inherited by fork()ed children, but exec without fork is
    if (Libc.fcntl(cap, fd, Libc.F_SETFD, Libc.FD_CLOEXEC) < 0) {
      val err = Libc.errno(cap)
      Libc.close(cap, fd)
      throw new IOException(s"fcntl(FD_CLOEXEC) failed: errno $err")
    }
    fd
  }

  def keventChange(
      kq: Int,
      ident: Long,
      filter: Short,
      flags: Int,
      fflags: Int,
      udata: Long): Int = {
    val s = scratch.get()
    s.set(ValueLayout.JAVA_LONG, KEVENT_IDENT, ident)
    s.set(ValueLayout.JAVA_SHORT, KEVENT_FILTER, filter)
    s.set(ValueLayout.JAVA_SHORT, KEVENT_FLAGS, flags.toShort)
    s.set(ValueLayout.JAVA_INT, KEVENT_FFLAGS, fflags)
    s.set(ValueLayout.JAVA_LONG, KEVENT_DATA, 0L)
    s.set(ValueLayout.JAVA_LONG, KEVENT_UDATA, udata)
    val c = Libc.capture()
    val change = s.asSlice(0L, KEVENT_SIZE)
    val zeroTimeout = s.asSlice(KEVENT_SIZE, 16L)
    if (Kqueue.keventNow(c, kq, change, 1, MemorySegment.NULL, 0, zeroTimeout) >= 0) 0
    else Libc.errno(c)
  }

  def keventWait(kq: Int, nanos: Long): Int = {
    val rtn =
      if (nanos == 0) Kqueue.keventNow(cap, kq, MemorySegment.NULL, 0, events, maxEvents, zero)
      else if (nanos < 0)
        Kqueue.kevent(cap, kq, MemorySegment.NULL, 0, events, maxEvents, MemorySegment.NULL)
      else {
        val secs = nanos / 1000000000L
        if (secs >= MaxTimeoutSeconds) {
          timespec.set(ValueLayout.JAVA_LONG, 0L, MaxTimeoutSeconds)
          timespec.set(ValueLayout.JAVA_LONG, 8L, 0L)
        } else {
          timespec.set(ValueLayout.JAVA_LONG, 0L, secs)
          timespec.set(ValueLayout.JAVA_LONG, 8L, nanos % 1000000000L)
        }
        Kqueue.kevent(cap, kq, MemorySegment.NULL, 0, events, maxEvents, timespec)
      }
    if (rtn >= 0) rtn else -Libc.errno(cap)
  }

  def eventFilter(i: Int): Short =
    events.get(ValueLayout.JAVA_SHORT, i * KEVENT_SIZE + KEVENT_FILTER)
  def eventFlags(i: Int): Int =
    events.get(ValueLayout.JAVA_SHORT, i * KEVENT_SIZE + KEVENT_FLAGS) & 0xffff
  def eventData(i: Int): Long =
    events.get(ValueLayout.JAVA_LONG, i * KEVENT_SIZE + KEVENT_DATA)
  def eventUdata(i: Int): Long =
    events.get(ValueLayout.JAVA_LONG, i * KEVENT_SIZE + KEVENT_UDATA)

  def close(fd: Int): Unit = {
    Libc.close(cap, fd)
    ()
  }

  def release(): Unit = arena.close()
}

private[ffm] object FfmKqueueSyscalls {

  private final val MaxTimeoutSeconds = cats.effect.unsafe.KqueueConstants.MaxTimeoutSeconds

  /**
   * A change record followed by a zero timespec, per thread; freed with the thread.
   */
  private val scratch: ThreadLocal[MemorySegment] =
    ThreadLocal.withInitial(() => Arena.ofAuto().allocate(KEVENT_SIZE + 16L, 8L))
}
