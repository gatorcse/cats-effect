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

  def add(fd: Int, reads: Boolean, writes: Boolean, id: Long): Int = {
    val err =
      if (reads) sys.keventChange(kq, fd.toLong, EVFILT_READ, EV_ADD | EV_CLEAR, 0, id) else 0
    if (err != 0 || !writes) err
    else {
      val werr = sys.keventChange(kq, fd.toLong, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, id)
      if (werr != 0 && reads) {
        sys.keventChange(kq, fd.toLong, EVFILT_READ, EV_DELETE, 0, 0L)
        ()
      }
      werr
    }
  }

  def remove(fd: Int, reads: Boolean, writes: Boolean): Unit = {
    if (reads) { sys.keventChange(kq, fd.toLong, EVFILT_READ, EV_DELETE, 0, 0L); () }
    if (writes) { sys.keventChange(kq, fd.toLong, EVFILT_WRITE, EV_DELETE, 0, 0L); () }
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

  private[this] val oneShots =
    new ConcurrentHashMap[java.lang.Long, Either[Throwable, Long] => Unit]
  private[this] val nextOneShotId = new AtomicLong(WakeupId)

  /**
   * Adds a one-shot `(ident, filter)` event that completes `cb` with the event's `data`. Owner
   * thread. Returns the id to pass to [[cancelOneShot]].
   */
  private[unsafe] def addOneShot(
      ident: Int,
      filter: Short,
      flags: Int,
      fflags: Int,
      cb: Either[Throwable, Long] => Unit): Long = {
    val id = nextOneShotId.decrementAndGet()
    oneShots.put(id, cb)
    val err =
      sys.keventChange(kq, ident.toLong, filter, flags | EV_ADD | EV_ONESHOT, fflags, id)
    if (err != 0 && (oneShots.remove(id) ne null))
      cb(Left(new IOException(s"kevent(ident $ident, filter $filter) failed: errno $err")))
    id
  }

  /**
   * Forgets a one-shot event; if it fires later it is ignored. Any thread.
   */
  private[unsafe] def cancelOneShot(id: Long): Unit = {
    oneShots.remove(id)
    ()
  }

  def dispatchOther(i: Int, id: Long): Boolean = {
    val cb = oneShots.remove(id)
    if (cb eq null) false
    else {
      val data = sys.eventData(i)
      if ((sys.eventFlags(i) & EV_ERROR) != 0)
        cb(Left(new IOException(s"kevent failed: errno $data")))
      else cb(Right(data))
      true
    }
  }

  def hasOther: Boolean = !oneShots.isEmpty

  def close(): Unit = {
    sys.close(kq)
    sys.release()
  }
}
