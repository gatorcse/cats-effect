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

import cats.effect.unsafe.metrics.PollerMetrics

import java.io.IOException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import EpollConstants._

/**
 * The epoll event loop shared by the JVM (`cats-effect-ffm`'s `FfmEpollSystem`) and Scala
 * Native (`EpollSystem`): handle table, wake-ups, lifecycle and event dispatch. It calls the OS
 * only through [[EpollSyscalls]].
 */
private[effect] object EpollEventLoop {

  /**
   * `epoll_data` of the poller's own wake-up eventfd. Never a valid handle id.
   */
  private final val WakeupId = -1L

  private def errnoException(op: String, errno: Int): IOException =
    new IOException(s"$op failed: errno $errno")

  final class PollHandle private[EpollEventLoop] (
      val poller: Poller,
      val fd: Int,
      val slot: Int,
      val id: Long,
      reads: Boolean,
      writes: Boolean
  ) extends EdgeTriggeredPollHandle {

    /**
     * Set once, by whichever thread deregisters first.
     */
    private[effect] val deregisteredFlag = new AtomicBoolean(false)
    private[effect] def deregistered: Boolean = deregisteredFlag.get()

    private[this] val readState =
      new Readiness(poller.readSubmitted, poller.readSucceeded, poller.readCanceled)
    private[this] val writeState =
      new Readiness(poller.writeSubmitted, poller.writeSucceeded, poller.writeCanceled)

    private[this] val readLock = new PollHandleLock
    private[this] val writeLock = new PollHandleLock

    def readReadyCount: Int = readState.count
    def writeReadyCount: Int = writeState.count

    /**
     * Called by the owning poller's thread with the event flags.
     */
    private[EpollEventLoop] def notify(events: Int): Unit = {
      if (reads && (events & ReadWake) != 0) readState.fire()
      if (writes && (events & WriteWake) != 0) writeState.fire()
    }

    def awaitReadReady(seen: Int): IO[Int] = readState.await(seen)

    def awaitWriteReady(seen: Int): IO[Int] = writeState.await(seen)

    def pollReadRec[A, B](a: A)(f: A => IO[Either[A, B]]): IO[B] =
      readLock.surround {
        def go(a: A, before: Int): IO[B] =
          f(a).flatMap {
            case Left(a) => readState.await(before).flatMap(go(a, _))
            case Right(b) => IO.pure(b)
          }
        IO(readState.count).flatMap(go(a, _))
      }

    def pollWriteRec[A, B](a: A)(f: A => IO[Either[A, B]]): IO[B] =
      writeLock.surround {
        def go(a: A, before: Int): IO[B] =
          f(a).flatMap {
            case Left(a) => writeState.await(before).flatMap(go(a, _))
            case Right(b) => IO.pure(b)
          }
        IO(writeState.count).flatMap(go(a, _))
      }

    private[effect] def deregister(): Unit = poller.deregister(this)
  }

  /**
   * One direction of a handle: a ready counter bumped by the owner on each edge, and at most
   * one waiting callback. The callback slot is only ever taken with an atomic swap or CAS, so
   * the owner firing, a waiter re-checking, and a canceled waiter's finalizer can each claim a
   * given callback at most once and can never clear a newer waiter's callback.
   */
  private final class Readiness(
      submitted: AtomicLong,
      succeeded: AtomicLong,
      canceled: AtomicLong) {

    @volatile private[this] var counter = 0
    private[this] val callback = new AtomicReference[Either[Throwable, Int] => Unit]

    def count: Int = counter

    /**
     * Owner thread only.
     */
    def fire(): Unit = {
      val next = counter + 1
      counter = next
      val cb = callback.getAndSet(null)
      if (cb ne null) {
        succeeded.incrementAndGet()
        cb(Right(next))
      }
    }

    def await(seen: Int): IO[Int] =
      IO.asyncCheckAttempt[Int] { cb =>
        IO {
          val now = counter
          if (now != seen) Right(now)
          else {
            submitted.incrementAndGet()
            callback.set(cb)
            val again = counter // re-check after publishing the callback
            if (again != seen) {
              // if the owner already took cb it completes it too; the second completion is a
              // no-op, and it has counted the success
              if (callback.compareAndSet(cb, null)) succeeded.incrementAndGet()
              Right(again)
            } else
              Left(Some(IO {
                if (callback.compareAndSet(cb, null)) canceled.incrementAndGet()
                ()
              }))
          }
        }
      }
  }

  private final val ReadWake = EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR
  // not RDHUP: a peer that stopped writing can still be written to
  private final val WriteWake = EPOLLOUT | EPOLLHUP | EPOLLERR

  /**
   * Generations use 31 bits so that no id is ever [[WakeupId]].
   */
  private final val GenerationMask = 0x7fffffff

  class Poller private[effect] (sys: EpollSyscalls) extends PollerMetrics {

    private[this] val maxEvents = sys.maxEvents
    private[this] var readyEventCount: Int = 0

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
        throw errnoException("epoll_ctl(eventfd)", err)
      }
      fd
    }

    // --- lifecycle: open while >= 0; close() adds Int.MinValue and waits for in-flight users --

    private[this] val inFlight = new AtomicInteger(0)

    /**
     * Runs `f` only if the poller is open, and keeps it open until `f` returns.
     */
    @inline private[this] def whileOpen(f: => Unit): Unit = {
      if (inFlight.incrementAndGet() > 0) {
        try f
        finally { inFlight.decrementAndGet(); () }
      } else {
        inFlight.decrementAndGet()
        ()
      }
    }

    private[effect] def close(): Unit = {
      if (inFlight.getAndAdd(Int.MinValue) >= 0) {
        while (inFlight.get() != Int.MinValue) Thread.`yield`()
        sys.close(wakeFd)
        sys.close(epfd)
        sys.release()
      }
    }

    def isClosed: Boolean = inFlight.get() < 0

    // --- wake-ups ------------------------------------------------------------------------------

    private[this] val wakeupPending = new AtomicBoolean(false)
    private[this] val wakeupWrites = new AtomicLong(0L)

    private[effect] def interrupt(): Unit =
      if (!wakeupPending.get() && wakeupPending.compareAndSet(false, true)) whileOpen {
        wakeupWrites.incrementAndGet()
        sys.eventfdSignal(wakeFd)
      }

    /**
     * Number of eventfd writes so far; for tests of wake-up debouncing.
     */
    private[effect] def wakeupWriteCount: Long = wakeupWrites.get()

    // --- handle table (owner thread only, except pendingFree) ---------------------------------

    private[this] var handles = new Array[PollHandle](64)
    private[this] var generations = new Array[Int](64)
    private[this] var freeSlots = new Array[Int](64)
    private[this] var freeCount = 0
    private[this] var nextSlot = 0

    /**
     * Slots released by any thread, reclaimed by the owner between batches.
     */
    private[this] val pendingFree = new ConcurrentLinkedQueue[PollHandle]
    private[this] val registered = new AtomicInteger(0)

    private[this] def reclaim(): Unit = {
      var h = pendingFree.poll()
      while (h ne null) {
        val slot = h.slot
        if (handles(slot) eq h) {
          handles(slot) = null
          generations(slot) = (generations(slot) + 1) & GenerationMask
          if (freeCount == freeSlots.length)
            freeSlots = java.util.Arrays.copyOf(freeSlots, freeCount * 2)
          freeSlots(freeCount) = slot
          freeCount += 1
        }
        h = pendingFree.poll()
      }
    }

    private[this] def allocateSlot(): Int =
      if (freeCount > 0) {
        freeCount -= 1
        freeSlots(freeCount)
      } else {
        if (nextSlot == handles.length) {
          handles = java.util.Arrays.copyOf(handles, nextSlot * 2)
          generations = java.util.Arrays.copyOf(generations, nextSlot * 2)
        }
        val s = nextSlot
        nextSlot += 1
        s
      }

    private[effect] def register(
        fd: Int,
        reads: Boolean,
        writes: Boolean): Either[Throwable, PollHandle] = {
      reclaim()
      val slot = allocateSlot()
      val id = (generations(slot).toLong << 32) | (slot.toLong & 0xffffffffL)
      val handle = new PollHandle(this, fd, slot, id, reads, writes)
      val interest = EPOLLET |
        (if (reads) EPOLLIN | EPOLLRDHUP else 0) |
        (if (writes) EPOLLOUT else 0)
      val err = sys.epollAdd(epfd, fd, interest, id)
      if (err != 0) {
        freeSlots(freeCount) = slot // never published, generation unchanged
        freeCount += 1
        Left(errnoException(s"epoll_ctl(ADD, fd $fd)", err))
      } else {
        handles(slot) = handle
        registered.incrementAndGet()
        Right(handle)
      }
    }

    private[effect] def deregister(handle: PollHandle): Unit =
      if (handle.deregisteredFlag.compareAndSet(false, true)) {
        registered.decrementAndGet()
        whileOpen {
          // thread-safe; ENOENT/EBADF if the fd was already closed, which is fine
          sys.epollDel(epfd, handle.fd)
        }
        pendingFree.offer(handle)
        ()
      }

    // --- polling -------------------------------------------------------------------------------

    private[effect] def poll(nanos: Long): PollResult = {
      val rtn = sys.epollWait(epfd, nanos)

      if (rtn > 0) {
        readyEventCount = rtn
        if (rtn < maxEvents) PollResult.Complete else PollResult.Incomplete
      } else if (rtn == 0) {
        PollResult.Interrupted
      } else {
        val err = -rtn
        if (err == EINTR) PollResult.Interrupted // the worker recomputes its deadline
        else throw errnoException("epoll_wait", err)
      }
    }

    private[effect] def processReadyEvents(): Boolean = {
      var fibersRescheduled = false
      var i = 0
      while (i < readyEventCount) {
        val flags = sys.eventFlags(i)
        val id = sys.eventData(i)
        if (id == WakeupId) {
          // Drain first, then re-arm. The other order loses a wake-up: an interrupt between
          // the reset and the drain writes, the drain eats its write, and the flag stays set,
          // so every later interrupt is skipped while the owner parks. An interrupt that falls
          // between the drain and the reset is skipped, which is fine: the owner is awake and
          // looks for work before it parks again. The eventfd is edge-triggered, so a write
          // that lands after this batch is reported by the next poll.
          sys.eventfdDrain(wakeFd)
          wakeupPending.set(false)
        } else {
          val slot = id.toInt
          val generation = (id >>> 32).toInt
          if (slot >= 0 && slot < handles.length) {
            val handle = handles(slot)
            // drop events for removed registrations and for reused slots
            if ((handle ne null) && !handle.deregistered && generations(slot) == generation) {
              handle.notify(flags)
              fibersRescheduled = true
            }
          }
        }
        i += 1
      }
      readyEventCount = 0
      reclaim() // only now: events earlier in this batch may still name the old slot
      fibersRescheduled
    }

    private[effect] def needsPoll(): Boolean = registered.get() > 0

    /**
     * Number of live registrations.
     */
    def registrationCount: Int = registered.get()

    // --- metrics ------------------------------------------------------------------------------

    private[effect] val readSubmitted = new AtomicLong(0L)
    private[effect] val readSucceeded = new AtomicLong(0L)
    private[effect] val readCanceled = new AtomicLong(0L)
    private[effect] val writeSubmitted = new AtomicLong(0L)
    private[effect] val writeSucceeded = new AtomicLong(0L)
    private[effect] val writeCanceled = new AtomicLong(0L)

    def readOperationsOutstandingCount(): Int =
      (readSubmitted.get() - readSucceeded.get() - readCanceled.get()).toInt
    def totalReadOperationsSubmittedCount(): Long = readSubmitted.get()
    def totalReadOperationsSucceededCount(): Long = readSucceeded.get()
    def totalReadOperationsErroredCount(): Long = 0L
    def totalReadOperationsCanceledCount(): Long = readCanceled.get()

    def writeOperationsOutstandingCount(): Int =
      (writeSubmitted.get() - writeSucceeded.get() - writeCanceled.get()).toInt
    def totalWriteOperationsSubmittedCount(): Long = writeSubmitted.get()
    def totalWriteOperationsSucceededCount(): Long = writeSucceeded.get()
    def totalWriteOperationsErroredCount(): Long = 0L
    def totalWriteOperationsCanceledCount(): Long = writeCanceled.get()

    def operationsOutstandingCount(): Int =
      readOperationsOutstandingCount() + writeOperationsOutstandingCount()
    def totalOperationsSubmittedCount(): Long =
      totalReadOperationsSubmittedCount() + totalWriteOperationsSubmittedCount()
    def totalOperationsSucceededCount(): Long =
      totalReadOperationsSucceededCount() + totalWriteOperationsSucceededCount()
    def totalOperationsErroredCount(): Long = 0L
    def totalOperationsCanceledCount(): Long =
      totalReadOperationsCanceledCount() + totalWriteOperationsCanceledCount()

    // accept and connect are reads and writes at this level
    def acceptOperationsOutstandingCount(): Int = 0
    def totalAcceptOperationsSubmittedCount(): Long = 0L
    def totalAcceptOperationsSucceededCount(): Long = 0L
    def totalAcceptOperationsErroredCount(): Long = 0L
    def totalAcceptOperationsCanceledCount(): Long = 0L
    def connectOperationsOutstandingCount(): Int = 0
    def totalConnectOperationsSubmittedCount(): Long = 0L
    def totalConnectOperationsSucceededCount(): Long = 0L
    def totalConnectOperationsErroredCount(): Long = 0L
    def totalConnectOperationsCanceledCount(): Long = 0L

    override def toString: String = "Epoll"
  }
}
