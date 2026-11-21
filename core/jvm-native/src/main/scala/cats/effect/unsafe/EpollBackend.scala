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

import java.io.IOException

import EpollConstants._
import EventLoopBackend.{Readable, WakeupId, Writable}

/**
 * epoll for the [[EventLoop]]: one epoll instance plus a non-blocking eventfd for wake-ups.
 * Takes ownership of `sys`.
 */
private[effect] final class EpollBackend(sys: EpollSyscalls) extends EventLoopBackend {
  import EpollBackend._

  def name: String = "epoll"

  def maxEvents: Int = sys.maxEvents

  private[this] val epfd: Int =
    try sys.epollCreate()
    catch { case t: Throwable => sys.release(); throw t }

  private[this] val wakeFd: Int = {
    val fd =
      try sys.eventfdCreate()
      catch { case t: Throwable => sys.close(epfd); sys.release(); throw t }
    val err = sys.epollAdd(epfd, fd, EPOLLIN | EPOLLET, WakeupId)
    if (err != 0) {
      sys.close(fd)
      sys.close(epfd)
      sys.release()
      throw new IOException(s"epoll_ctl(eventfd) failed: errno $err")
    }
    fd
  }

  def add(fd: Int, reads: Boolean, writes: Boolean, id: Long): Int = {
    val interest = EPOLLET |
      (if (reads) EPOLLIN | EPOLLRDHUP else 0) |
      (if (writes) EPOLLOUT else 0)
    sys.epollAdd(epfd, fd, interest, id)
  }

  def remove(fd: Int, reads: Boolean, writes: Boolean): Unit = sys.epollDel(epfd, fd)

  def poll(nanos: Long): Int = sys.epollWait(epfd, nanos)

  def eventId(i: Int): Long = sys.eventData(i)

  def eventReadiness(i: Int): Int = {
    val flags = sys.eventFlags(i)
    (if ((flags & ReadWake) != 0) Readable else 0) | (if ((flags & WriteWake) != 0) Writable
                                                      else 0)
  }

  def wake(): Boolean = sys.eventfdSignal(wakeFd)

  /**
   * epoll has no events other than handles' and the wake-up.
   */
  def dispatchOther(i: Int, id: Long): Boolean = false

  def hasOther: Boolean = false

  def clearWake(): Unit = sys.eventfdDrain(wakeFd)

  def close(): Unit = {
    sys.close(wakeFd)
    sys.close(epfd)
    sys.release()
  }
}

private object EpollBackend {
  private final val ReadWake = EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR
  // not RDHUP: a peer that stopped writing can still be written to
  private final val WriteWake = EPOLLOUT | EPOLLHUP | EPOLLERR
}
