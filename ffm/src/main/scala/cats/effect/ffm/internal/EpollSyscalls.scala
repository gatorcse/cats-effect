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

/**
 * Everything the epoll event loop needs from the OS, and nothing else.
 *
 * The event loop ([[EpollEventLoop]]) is written only against this interface so that it can move
 * to shared `jvm-native` code later, with an FFM implementation on the JVM
 * ([[FfmEpollSyscalls]]) and an `@extern` one on Scala Native. It deals only in ints and longs:
 * no `MemorySegment`, no `Ptr`.
 *
 * One instance belongs to one poller and owns that poller's native memory (event array,
 * timespec, errno scratch). Methods marked "any thread" may be called concurrently with the
 * owner; all others are called only by the thread that currently owns the poller.
 */
private[ffm] trait EpollSyscalls {

  /** `epoll_create1(EPOLL_CLOEXEC)`; throws `IOException` on failure. */
  def epollCreate(): Int

  /** A non-blocking, close-on-exec `eventfd`; throws `IOException` on failure. */
  def eventfdCreate(): Int

  /** `epoll_ctl(ADD)`; returns 0 or the errno. */
  def epollAdd(epfd: Int, fd: Int, events: Int, data: Long): Int

  /** `epoll_ctl(DEL)`; errors are ignored. Any thread. */
  def epollDel(epfd: Int, fd: Int): Unit

  /**
   * Waits for up to [[maxEvents]] events into this instance's event array. `nanos` is -1
   * (forever), 0 (don't block) or a positive timeout. Returns the event count, or `-errno`.
   */
  def epollWait(epfd: Int, nanos: Long): Int

  /** The flags of ready event `i` from the last [[epollWait]]. */
  def eventFlags(i: Int): Int

  /** The `epoll_data` of ready event `i` from the last [[epollWait]]. */
  def eventData(i: Int): Long

  def maxEvents: Int

  /** Adds 1 to an eventfd. Any thread. */
  def eventfdSignal(fd: Int): Unit

  /** Resets an eventfd to 0. */
  def eventfdDrain(fd: Int): Unit

  def close(fd: Int): Unit

  /** Frees this instance's native memory. Called once, last. */
  def release(): Unit
}

/** Linux epoll ABI constants, identical on every architecture. */
private[ffm] object EpollConstants {
  final val EPOLLIN = 0x001
  final val EPOLLOUT = 0x004
  final val EPOLLERR = 0x008
  final val EPOLLHUP = 0x010
  final val EPOLLRDHUP = 0x2000
  final val EPOLLET = 1 << 31
  final val EINTR = 4
}
