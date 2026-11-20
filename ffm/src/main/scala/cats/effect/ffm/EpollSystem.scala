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
package ffm

import cats.effect.ffm.internal.Libc
import cats.effect.std.Mutex
import cats.effect.unsafe.{PollResult, PollingContext, PollingSystem}
import cats.effect.unsafe.metrics.PollerMetrics
import cats.syntax.all._

import java.io.IOException
import java.lang.foreign.{Arena, MemorySegment, ValueLayout}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReferenceArray}

/**
 * A [[cats.effect.unsafe.PollingSystem PollingSystem]] backed by Linux `epoll`, called directly
 * through the Foreign Function & Memory API. A port of the Scala Native `EpollSystem`.
 *
 * Each worker thread owns one epoll instance (its `Poller`). Registrations are edge-triggered;
 * readiness is published to a [[FileDescriptorPollHandle]] as a counter so that a fiber that
 * races a notification never misses it.
 */
final class EpollSystem private (maxEvents: Int) extends PollingSystem {

  import EpollSystem._

  type Api = FileDescriptorPoller
  type Poller = EpollSystem.Poller

  def close(): Unit = ()

  def makeApi(ctx: PollingContext[Poller]): Api = new FileDescriptorPollerImpl(ctx)

  def makePoller(): Poller = new Poller(maxEvents)

  def closePoller(poller: Poller): Unit = poller.close()

  def poll(poller: Poller, nanos: Long): PollResult = poller.poll(nanos)

  def processReadyEvents(poller: Poller): Boolean = poller.processReadyEvents()

  def needsPoll(poller: Poller): Boolean = poller.needsPoll()

  def interrupt(targetThread: Thread, targetPoller: Poller): Unit = targetPoller.interrupt()

  def metrics(poller: Poller): PollerMetrics = poller

  private final class FileDescriptorPollerImpl(ctx: PollingContext[Poller])
      extends FileDescriptorPoller {

    def registerFileDescriptor(
        fd: Int,
        reads: Boolean,
        writes: Boolean
    ): Resource[IO, FileDescriptorPollHandle] =
      Resource {
        (
          if (handleMutex) (Mutex[IO], Mutex[IO]).tupled.map { case (r, w) => (r.lock, w.lock) }
          else IO.pure((null: Resource[IO, Unit], null: Resource[IO, Unit]))
        ).flatMap { case (readMutex, writeMutex) =>
          IO.async_[(FileDescriptorPollHandle, IO[Unit])] { cb =>
            ctx.accessPoller { poller =>
              val handle = new PollHandle(readMutex, writeMutex)
              cb(poller.register(fd, reads, writes, handle))
            }
          }
        }
      }
  }
}

object EpollSystem {

  final val DefaultMaxEvents = 64

  /**
   * Spike knob: wrap each handle's `pollReadRec`/`pollWriteRec` in a `Mutex` (as Scala Native
   * does) so concurrent readers/writers are serialized. Set
   * `-Dcats.effect.ffm.handleMutex=false` when the caller already guarantees a single reader
   * and a single writer (fs2's sockets have their own mutexes) to measure the mutex's cost.
   */
  private val handleMutex: Boolean =
    !"false".equals(System.getProperty("cats.effect.ffm.handleMutex"))

  def apply(): EpollSystem = new EpollSystem(DefaultMaxEvents)

  def apply(maxEvents: Int): EpollSystem = new EpollSystem(maxEvents)

  /** epoll_data value used to tag the poller's own wake-up eventfd. */
  private final val InterruptTag = -1L

  /** A constant 8-byte `1L`, written to an eventfd to wake a poller. Read-only, so shareable. */
  private val One: MemorySegment = {
    val s = Arena.global().allocate(8L)
    s.set(ValueLayout.JAVA_LONG, 0L, 1L)
    s
  }

  private final class PollHandle(readMutex: Resource[IO, Unit], writeMutex: Resource[IO, Unit])
      extends FileDescriptorPollHandle {

    @volatile private[this] var readReadyCounter = 0
    @volatile private[this] var readCallback: Either[Throwable, Int] => Unit = null

    @volatile private[this] var writeReadyCounter = 0
    @volatile private[this] var writeCallback: Either[Throwable, Int] => Unit = null

    def readReadyCount: Int = readReadyCounter

    def awaitReadReady(seen: Int): IO[Int] =
      IO(readReadyCounter).flatMap { now =>
        if (now != seen) IO.pure(now)
        else
          IO.asyncCheckAttempt[Int] { cb =>
            IO {
              readCallback = cb
              val now = readReadyCounter
              if (now != seen) {
                readCallback = null
                Right(now)
              } else Left(Some(IO(this.readCallback = null)))
            }
          }
      }

    @inline private[this] def locked[B](lock: Resource[IO, Unit])(body: IO[B]): IO[B] =
      if (lock eq null) body else lock.surround(body)

    def notify(events: Int): Unit = {
      if ((events & (Libc.EPOLLIN | Libc.EPOLLHUP | Libc.EPOLLERR)) != 0) {
        val counter = readReadyCounter + 1
        readReadyCounter = counter
        val cb = readCallback
        readCallback = null
        if (cb ne null) cb(Right(counter))
      }
      if ((events & (Libc.EPOLLOUT | Libc.EPOLLHUP | Libc.EPOLLERR)) != 0) {
        val counter = writeReadyCounter + 1
        writeReadyCounter = counter
        val cb = writeCallback
        writeCallback = null
        if (cb ne null) cb(Right(counter))
      }
    }

    def pollReadRec[A, B](a: A)(f: A => IO[Either[A, B]]): IO[B] =
      locked(readMutex) {
        def go(a: A, before: Int): IO[B] =
          f(a).flatMap {
            case Left(a) =>
              IO(readReadyCounter).flatMap { after =>
                if (before != after) go(a, after)
                else
                  IO.asyncCheckAttempt[Int] { cb =>
                    IO {
                      readCallback = cb
                      val now = readReadyCounter
                      if (now != before) {
                        readCallback = null
                        Right(now)
                      } else Left(Some(IO(this.readCallback = null)))
                    }
                  }.flatMap(go(a, _))
              }
            case Right(b) => IO.pure(b)
          }

        IO(readReadyCounter).flatMap(go(a, _))
      }

    def pollWriteRec[A, B](a: A)(f: A => IO[Either[A, B]]): IO[B] =
      locked(writeMutex) {
        def go(a: A, before: Int): IO[B] =
          f(a).flatMap {
            case Left(a) =>
              IO(writeReadyCounter).flatMap { after =>
                if (before != after) go(a, after)
                else
                  IO.asyncCheckAttempt[Int] { cb =>
                    IO {
                      writeCallback = cb
                      val now = writeReadyCounter
                      if (now != before) {
                        writeCallback = null
                        Right(now)
                      } else Left(Some(IO(this.writeCallback = null)))
                    }
                  }.flatMap(go(a, _))
              }
            case Right(b) => IO.pure(b)
          }

        IO(writeReadyCounter).flatMap(go(a, _))
      }
  }

  final class Poller private[EpollSystem] (maxEvents: Int) extends PollerMetrics {

    private[this] val arena: Arena = Arena.ofShared()
    // owner-thread scratch
    private[this] val capture: MemorySegment = Libc.allocateCapture(arena)
    private[this] val ctlEvent: MemorySegment = arena.allocate(Libc.EPOLL_EVENT_SIZE, 8L)
    private[this] val drainBuf: MemorySegment = arena.allocate(8L, 8L)
    private[this] val events: MemorySegment =
      arena.allocate(Libc.EPOLL_EVENT_SIZE * maxEvents, 8L)
    private[this] var readyEventCount: Int = 0

    private[this] val epfd: Int = {
      val fd = Libc.epollCreate1(Libc.EFD_CLOEXEC)
      if (fd < 0) throw new IOException("epoll_create1 failed")
      fd
    }

    /**
     * Handles indexed by fd. Written on registration (owner thread) and on deregistration (any
     * thread); read lock-free by the owner thread when dispatching events.
     */
    @volatile private[this] var handles = new AtomicReferenceArray[PollHandle](1024)
    private[this] val registered = new AtomicInteger(0)

    private[this] val interruptFd: Int = {
      val fd = Libc.eventfd(0, Libc.EFD_NONBLOCK | Libc.EFD_CLOEXEC)
      if (fd < 0) throw new IOException("eventfd failed")
      setEvent(Libc.EPOLLET | Libc.EPOLLIN, InterruptTag)
      if (Libc.epollCtl(capture, epfd, Libc.EPOLL_CTL_ADD, fd, ctlEvent) != 0)
        throw new IOException(s"epoll_ctl(eventfd) failed: errno ${Libc.errno(capture)}")
      fd
    }

    private[this] def setEvent(events: Int, data: Long): Unit = {
      ctlEvent.set(ValueLayout.JAVA_INT, 0L, events)
      ctlEvent.set(Libc.JAVA_LONG_UNALIGNED, Libc.EPOLL_EVENT_DATA_OFFSET, data)
    }

    private[EpollSystem] def close(): Unit = {
      Libc.close(interruptFd)
      Libc.close(epfd)
      arena.close()
    }

    private[EpollSystem] def poll(nanos: Long): PollResult = {
      val timeoutMillis = if (nanos == -1) -1 else (nanos / 1000000L).toInt
      val rtn =
        if (timeoutMillis == 0) Libc.epollWaitNow(capture, epfd, events, maxEvents)
        else Libc.epollWait(capture, epfd, events, maxEvents, timeoutMillis)
      if (rtn > 0) {
        readyEventCount = rtn
        if (rtn < maxEvents) PollResult.Complete else PollResult.Incomplete
      } else if (rtn == 0) {
        PollResult.Interrupted
      } else {
        val err = Libc.errno(capture)
        if (err == Libc.EINTR) PollResult.Interrupted
        else throw new IOException(s"epoll_wait failed: errno $err")
      }
    }

    private[EpollSystem] def processReadyEvents(): Boolean = {
      var fibersRescheduled = false
      val hs = handles
      var i = 0
      while (i < readyEventCount) {
        val base = i * Libc.EPOLL_EVENT_SIZE
        val flags = events.get(ValueLayout.JAVA_INT, base)
        val data = events.get(Libc.JAVA_LONG_UNALIGNED, base + Libc.EPOLL_EVENT_DATA_OFFSET)
        if (data == InterruptTag) {
          Libc.read(interruptFd, drainBuf, 8L)
        } else {
          val fd = data.toInt
          val handle = if (fd < hs.length()) hs.get(fd) else null
          if (handle ne null) {
            if ((flags & Libc.EPOLLIN) != 0) totalReadSucceeded += 1
            if ((flags & Libc.EPOLLOUT) != 0) totalWriteSucceeded += 1
            handle.notify(flags)
            fibersRescheduled = true
          }
        }
        i += 1
      }
      readyEventCount = 0
      fibersRescheduled
    }

    private[EpollSystem] def needsPoll(): Boolean = registered.get() > 0

    private[EpollSystem] def interrupt(): Unit = {
      Libc.write(interruptFd, One, 8L)
      ()
    }

    private[EpollSystem] def register(
        fd: Int,
        reads: Boolean,
        writes: Boolean,
        handle: PollHandle
    ): Either[Throwable, (FileDescriptorPollHandle, IO[Unit])] = {
      put(fd, handle)
      setEvent(
        Libc.EPOLLET | (if (reads) Libc.EPOLLIN | Libc.EPOLLRDHUP else 0) |
          (if (writes) Libc.EPOLLOUT else 0),
        fd.toLong)
      if (Libc.epollCtl(capture, epfd, Libc.EPOLL_CTL_ADD, fd, ctlEvent) != 0) {
        remove(fd, handle)
        Left(new IOException(s"epoll_ctl(ADD, $fd) failed: errno ${Libc.errno(capture)}"))
      } else {
        registered.incrementAndGet()
        if (reads) { totalReadSubmitted += 1 }
        if (writes) { totalWriteSubmitted += 1 }
        val unregister = IO {
          // safe from any thread: epoll_ctl is thread-safe and the slot is CAS-cleared
          Libc.epollCtlDel(epfd, fd)
          remove(fd, handle)
          registered.decrementAndGet()
          ()
        }
        Right((handle, unregister))
      }
    }

    private[this] def put(fd: Int, handle: PollHandle): Unit = {
      var hs = handles
      if (fd >= hs.length()) synchronized {
        hs = handles
        if (fd >= hs.length()) {
          var len = hs.length()
          while (len <= fd) len *= 2
          val grown = new AtomicReferenceArray[PollHandle](len)
          var i = 0
          while (i < hs.length()) { grown.set(i, hs.get(i)); i += 1 }
          handles = grown
          hs = grown
        }
      }
      hs.set(fd, handle)
    }

    private[this] def remove(fd: Int, handle: PollHandle): Unit = synchronized {
      val hs = handles
      if (fd < hs.length()) { hs.compareAndSet(fd, handle, null); () }
    }

    // --- metrics (best-effort, owner-thread counters) ---------------------------------------
    private[this] var totalReadSubmitted = 0L
    private[this] var totalReadSucceeded = 0L
    private[this] var totalWriteSubmitted = 0L
    private[this] var totalWriteSucceeded = 0L

    def operationsOutstandingCount(): Int = registered.get()
    def totalOperationsSubmittedCount(): Long = totalReadSubmitted + totalWriteSubmitted
    def totalOperationsSucceededCount(): Long = totalReadSucceeded + totalWriteSucceeded
    def totalOperationsErroredCount(): Long = 0L
    def totalOperationsCanceledCount(): Long = 0L
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
    def readOperationsOutstandingCount(): Int = 0
    def totalReadOperationsSubmittedCount(): Long = totalReadSubmitted
    def totalReadOperationsSucceededCount(): Long = totalReadSucceeded
    def totalReadOperationsErroredCount(): Long = 0L
    def totalReadOperationsCanceledCount(): Long = 0L
    def writeOperationsOutstandingCount(): Int = 0
    def totalWriteOperationsSubmittedCount(): Long = totalWriteSubmitted
    def totalWriteOperationsSucceededCount(): Long = totalWriteSucceeded
    def totalWriteOperationsErroredCount(): Long = 0L
    def totalWriteOperationsCanceledCount(): Long = 0L

    override def toString: String = "EpollFFM"
  }
}
