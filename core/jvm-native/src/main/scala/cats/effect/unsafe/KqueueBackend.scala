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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import EventLoopBackend.{Readable, WakeupId, Writable}
import KqueueConstants._

/**
 * kqueue for the [[EventLoop]]: `EV_CLEAR` (edge-triggered) read and write filters per handle,
 * and an `EVFILT_USER` event for wake-ups. Takes ownership of `sys`.
 *
 * It also runs one-shot filters for [[KqueueSystem]]'s `awaitEvent` on Scala Native: those are
 * keyed by negative ids below [[EventLoopBackend.WakeupId]].
 */
private[effect] final class KqueueBackend(sys: KqueueSyscalls) extends EventLoopBackend {

  def name: String = "kqueue"

  def maxEvents: Int = sys.maxEvents

  private[this] val kq: Int =
    try sys.kqueueCreate()
    catch { case t: Throwable => sys.release(); throw t }

  {
    val err = sys.keventChange(kq, 0L, sys.userFilter, EV_ADD | EV_CLEAR, 0, WakeupId)
    if (err != 0) {
      sys.close(kq)
      sys.release()
      throw new IOException(s"kevent(EVFILT_USER) failed: errno $err")
    }
  }

  // --- the (ident, filter) pairs in use -------------------------------------------------------
  //
  // kqueue keys an entry by (ident, filter): EV_ADD on a pair that already exists modifies it
  // (taking over its udata) instead of failing like epoll's EEXIST, and EV_DELETE removes it for
  // everyone. So each pair has one owner at a time, a handle direction or a one-shot, and a
  // second claim fails with EEXIST.

  private[this] val claimed = ConcurrentHashMap.newKeySet[java.lang.Long]()

  @inline private[this] def key(ident: Int, filter: Short): Long =
    (filter.toLong << 32) | (ident.toLong & 0xffffffffL)

  private[this] def claim(ident: Int, filter: Short): Boolean = claimed.add(key(ident, filter))

  private[this] def unclaim(ident: Int, filter: Short): Unit = {
    claimed.remove(key(ident, filter))
    ()
  }

  private[this] def addFilter(fd: Int, filter: Short, id: Long): Int =
    if (!claim(fd, filter)) EEXIST
    else {
      val err = sys.keventChange(kq, fd.toLong, filter, EV_ADD | EV_CLEAR, 0, id)
      if (err != 0) unclaim(fd, filter)
      err
    }

  private[this] def deleteFilter(fd: Int, filter: Short): Unit = {
    // delete before releasing the claim, so that nobody else's entry for the pair is deleted
    sys.keventChange(kq, fd.toLong, filter, EV_DELETE, 0, 0L)
    unclaim(fd, filter)
  }

  def add(fd: Int, reads: Boolean, writes: Boolean, id: Long): Int = {
    val err = if (reads) addFilter(fd, EVFILT_READ, id) else 0
    if (err != 0 || !writes) err
    else {
      val werr = addFilter(fd, EVFILT_WRITE, id)
      if (werr != 0 && reads) deleteFilter(fd, EVFILT_READ) // only what this call added
      werr
    }
  }

  /**
   * As with epoll, removal is by fd number: release a registration before closing its fd. If
   * the fd is closed first and its number reused, registering the new fd fails with EEXIST
   * until the old registration is released (rather than silently taking over or losing its
   * filters).
   */
  def remove(fd: Int, reads: Boolean, writes: Boolean): Unit = {
    if (reads) deleteFilter(fd, EVFILT_READ)
    if (writes) deleteFilter(fd, EVFILT_WRITE)
  }

  def poll(nanos: Long): Int = sys.keventWait(kq, nanos)

  def eventId(i: Int): Long = sys.eventUdata(i)

  def eventReadiness(i: Int): Int =
    if ((sys.eventFlags(i) & EV_ERROR) != 0) Readable | Writable
    else {
      // EV_EOF needs no special case: it arrives on the filter that should wake, and the
      // waiter then reads to EOF or gets EPIPE
      val filter = sys.eventFilter(i)
      if (filter == EVFILT_READ) Readable
      else if (filter == EVFILT_WRITE) Writable
      else 0
    }

  def wake(): Unit = {
    sys.keventChange(kq, 0L, sys.userFilter, 0, NOTE_TRIGGER, WakeupId)
    ()
  }

  /**
   * Nothing to do: the `EVFILT_USER` event is `EV_CLEAR`, so reporting it reset it.
   */
  def clearWake(): Unit = ()

  // --- one-shot filters, for KqueueSystem.awaitEvent ----------------------------------------

  private[this] val oneShots = new ConcurrentHashMap[java.lang.Long, KqueueBackend.OneShot]
  private[this] val nextOneShotId = new AtomicLong(WakeupId)

  /**
   * Adds a one-shot `(ident, filter)` event that completes `cb` with the event's `data`. Owner
   * thread. Fails `cb` with EEXIST if a handle or another one-shot already uses the pair.
   * Returns the id to pass to [[cancelOneShot]].
   */
  private[unsafe] def addOneShot(
      ident: Int,
      filter: Short,
      flags: Int,
      fflags: Int,
      cb: Either[Throwable, Long] => Unit): Long = {
    val id = nextOneShotId.decrementAndGet()
    if (!claim(ident, filter))
      cb(Left(new IOException(s"kevent(ident $ident, filter $filter) is already in use")))
    else {
      oneShots.put(id, new KqueueBackend.OneShot(ident, filter, cb))
      val err =
        sys.keventChange(kq, ident.toLong, filter, flags | EV_ADD | EV_ONESHOT, fflags, id)
      if (err != 0 && (oneShots.remove(id) ne null)) {
        unclaim(ident, filter)
        cb(Left(new IOException(s"kevent(ident $ident, filter $filter) failed: errno $err")))
      }
    }
    id
  }

  /**
   * Cancels a one-shot event that has not fired: removes it from the kqueue, so that it neither
   * fires later nor lingers for the next user of the pair. Any thread. Returns false if it had
   * already completed.
   */
  private[unsafe] def cancelOneShot(id: Long): Boolean = {
    val os = oneShots.remove(id)
    if (os ne null) deleteFilter(os.ident, os.filter)
    os ne null
  }

  def dispatchOther(i: Int, id: Long): Boolean = {
    val os = oneShots.remove(id)
    if (os eq null) false
    else {
      unclaim(os.ident, os.filter) // the kernel dropped the EV_ONESHOT entry when reporting it
      val data = sys.eventData(i)
      if ((sys.eventFlags(i) & EV_ERROR) != 0)
        os.cb(Left(new IOException(s"kevent failed: errno $data")))
      else os.cb(Right(data))
      true
    }
  }

  def hasOther: Boolean = !oneShots.isEmpty

  def close(): Unit = {
    sys.close(kq)
    sys.release()
  }
}

private object KqueueBackend {
  private final class OneShot(
      val ident: Int,
      val filter: Short,
      val cb: Either[Throwable, Long] => Unit)
}
