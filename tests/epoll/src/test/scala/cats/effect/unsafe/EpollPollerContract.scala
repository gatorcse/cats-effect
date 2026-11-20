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

import scala.concurrent.duration._

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import munit.FunSuite

/**
 * Contract tests that drive one epoll poller directly, without a runtime, so that event
 * batches, wake-ups and timeouts can be observed exactly. Each case names the Netty issue it
 * guards. The same cases run on the JVM (FFM) and on Scala Native (`@extern`).
 */
abstract class EpollPollerContract extends FunSuite with PollerTestPlatform {

  override def munitIgnore: Boolean = !epollSupported

  def withPoller[A](maxEvents: Int = 64, nanos: Boolean = true)(
      f: EpollEventLoop.Poller => A): A = {
    val poller = newPoller(maxEvents, nanos && epollPwait2Available)
    try f(poller)
    finally poller.close()
  }

  def newPoller(maxEvents: Int = 64, nanos: Boolean = false): EpollEventLoop.Poller =
    new EpollEventLoop.Poller(makeSyscalls(maxEvents, nanos))

  def register(p: EpollEventLoop.Poller, fd: Int, reads: Boolean, writes: Boolean) =
    p.register(fd, reads, writes).fold(throw _, identity)

  /**
   * Runs `poll` on another thread; the reference holds (result, elapsed nanos).
   */
  def pollOnThread(
      p: EpollEventLoop.Poller,
      nanos: Long): (AtomicReference[(PollResult, Long)], CountDownLatch) = {
    val result = new AtomicReference[(PollResult, Long)]
    val done = new CountDownLatch(1)
    val t = new Thread(new Runnable {
      def run(): Unit = {
        val start = System.nanoTime()
        val r = p.poll(nanos)
        result.set((r, System.nanoTime() - start))
        done.countDown()
      }
    })
    t.start()
    (result, done)
  }

  def thread(body: => Unit): Thread = new Thread(new Runnable { def run(): Unit = body })

  test("an event for a ready fd is delivered once (edge-triggered)") {
    withPoller() { p =>
      val (r, w) = pipe()
      try {
        val h = register(p, r, reads = true, writes = false)
        writeUnsafe(w, Array[Byte](1))
        assertEquals(p.poll(0), PollResult.Complete)
        assert(p.processReadyEvents())
        assertEquals(h.readReadyCount, 1)
        assertEquals(p.poll(0), PollResult.Interrupted) // no new edge
      } finally { close(r); close(w) }
    }
  }

  test("N interrupts between polls cause exactly one eventfd write (netty#9191)") {
    withPoller() { p =>
      val before = p.wakeupWriteCount
      val threads = (1 to 8).map(_ => thread((1 to 100).foreach(_ => p.interrupt())))
      threads.foreach(_.start())
      threads.foreach(_.join())
      assertEquals(p.wakeupWriteCount - before, 1L)
      // the pending wake-up makes the next park return at once, and re-arms the debounce
      assertEquals(p.poll(-1), PollResult.Complete)
      p.processReadyEvents()
      assertEquals(p.poll(0), PollResult.Interrupted, "eventfd was not drained")
      p.interrupt()
      p.interrupt()
      assertEquals(p.wakeupWriteCount - before, 2L)
    }
  }

  test("an interrupt wakes a parked poll") {
    withPoller() { p =>
      val (result, done) = pollOnThread(p, -1)
      Thread.sleep(50)
      assertEquals(done.getCount, 1L)
      p.interrupt()
      assert(done.await(5, TimeUnit.SECONDS), "poll was not woken")
      assertEquals(result.get()._1, PollResult.Complete)
    }
  }

  test("the owner thread never wakes itself") {
    val system = epollSystem()
    val p = system.makePoller()
    try {
      val before = p.wakeupWriteCount
      system.interrupt(Thread.currentThread(), p)
      assertEquals(p.wakeupWriteCount, before)
    } finally system.closePoller(p)
  }

  test("interrupts racing close never fail or write after close (netty#16836, #9586)") {
    (1 to 50).foreach { _ =>
      val p = newPoller()
      val start = new CountDownLatch(1)
      val failure = new AtomicReference[Throwable]
      val threads = (1 to 4).map { _ =>
        thread {
          start.await()
          try { (1 to 200).foreach(_ => p.interrupt()) }
          catch { case t: Throwable => failure.set(t) }
        }
      }
      threads.foreach(_.start())
      start.countDown()
      p.close()
      threads.foreach(_.join())
      val after = p.wakeupWriteCount
      p.interrupt()
      assertEquals(p.wakeupWriteCount, after, "interrupt wrote after close")
      assert(p.isClosed)
      if (failure.get() ne null) throw failure.get()
    }
  }

  for (nanos <- List(true, false))
    test(
      s"a 300us timed poll parks instead of spinning (netty#18137), nanosecondTimeouts=$nanos") {
      withPoller(nanos = nanos) { p =>
        val start = System.nanoTime()
        var polls = 0
        while (System.nanoTime() - start < 300000L) {
          p.poll(300000L - (System.nanoTime() - start))
          polls += 1
        }
        assert(polls <= 3, s"spun: $polls polls for one 300us deadline")
      }
    }

  for {
    nanos <- List(true, false)
    timeout <- List(30L * 24 * 3600 * 1000000000L, Long.MaxValue)
  } test(s"very long timeouts do not overflow (netty#18213, #13503), nanos=$nanos, $timeout") {
    withPoller(nanos = nanos) { p =>
      val (result, done) = pollOnThread(p, timeout)
      Thread.sleep(50)
      assertEquals(done.getCount, 1L, s"returned early: ${result.get()}")
      p.interrupt()
      assert(done.await(5, TimeUnit.SECONDS), "not woken")
      assertEquals(result.get()._1, PollResult.Complete)
    }
  }

  test("an event for a registration removed mid-batch is dropped (netty#15279)") {
    withPoller() { p =>
      val (r, w) = pipe()
      try {
        val h = register(p, r, reads = true, writes = false)
        writeUnsafe(w, Array[Byte](1))
        assertEquals(p.poll(0), PollResult.Complete) // event is now in the batch
        h.deregister()
        assert(!p.processReadyEvents())
        assertEquals(h.readReadyCount, 0)
      } finally { close(r); close(w) }
    }
  }

  test("a reused fd and slot never reach the old handle (netty#15150)") {
    withPoller() { p =>
      val (r1, w1) = pipe()
      val h1 = register(p, r1, reads = true, writes = false)
      writeUnsafe(w1, Array[Byte](1))
      assertEquals(p.poll(0), PollResult.Complete)
      h1.deregister()
      close(r1); close(w1)
      p.processReadyEvents() // reclaims h1's slot

      val (r2, w2) = pipe() // very likely the same fd numbers
      try {
        val h2 = register(p, r2, reads = true, writes = false)
        assertEquals(h2.slot, h1.slot, "expected the slot to be reused")
        assertNotEquals(h2.id, h1.id)
        writeUnsafe(w2, Array[Byte](2))
        assertEquals(p.poll(0), PollResult.Complete)
        p.processReadyEvents()
        assertEquals(h1.readReadyCount, 0)
        assertEquals(h2.readReadyCount, 1)
      } finally { close(r2); close(w2) }
    }
  }

  test("more ready fds than maxEvents are drained over Incomplete polls") {
    withPoller(maxEvents = 4) { p =>
      val pipes = (1 to 10).map(_ => pipe())
      try {
        val hs = pipes.map { case (r, _) => register(p, r, reads = true, writes = false) }
        pipes.foreach { case (_, w) => writeUnsafe(w, Array[Byte](1)) }
        var results = List.empty[PollResult]
        var r = p.poll(0)
        while (r ne PollResult.Interrupted) {
          results ::= r
          p.processReadyEvents()
          r = if (r eq PollResult.Incomplete) p.poll(0) else PollResult.Interrupted
        }
        assertEquals(
          results.reverse,
          List(PollResult.Incomplete, PollResult.Incomplete, PollResult.Complete))
        assert(hs.forall(_.readReadyCount == 1))
      } finally pipes.foreach { case (r, w) => close(r); close(w) }
    }
  }

  test("an unconnected socket does not cause an EPOLLHUP storm (netty#16250)") {
    withPoller() { p =>
      val fd = unconnectedTcpSocket()
      try {
        register(p, fd, reads = true, writes = true)
        var events = 0
        (1 to 20).foreach { _ =>
          if (p.poll(0) ne PollResult.Interrupted) { events += 1; p.processReadyEvents(); () }
        }
        assert(events <= 1, s"$events polls returned events")
      } finally close(fd)
    }
  }

  test("deregistering after the poller closed is harmless") {
    val p = newPoller()
    val (r, w) = pipe()
    try {
      val h = register(p, r, reads = true, writes = false)
      p.close()
      h.deregister()
      assertEquals(p.registrationCount, 0)
    } finally { close(r); close(w) }
  }

  test("metrics: submitted, succeeded and canceled waits") {
    import cats.effect.unsafe.implicits.global
    withPoller() { p =>
      val (r, w) = pipe()
      try {
        val h = register(p, r, reads = true, writes = false)
        // a wait that is canceled
        h.awaitReadReady(0).start.flatMap(f => IO.sleep(20.millis) *> f.cancel).unsafeRunSync()
        assertEquals(p.totalReadOperationsSubmittedCount(), 1L)
        assertEquals(p.totalReadOperationsCanceledCount(), 1L)
        assertEquals(p.readOperationsOutstandingCount(), 0)
        // a wait that succeeds
        val waiting = h.awaitReadReady(0).unsafeToFuture()
        Thread.sleep(20)
        assertEquals(p.readOperationsOutstandingCount(), 1)
        writeUnsafe(w, Array[Byte](1))
        p.poll(0)
        p.processReadyEvents()
        assertEquals(scala.concurrent.Await.result(waiting, 5.seconds), 1)
        assertEquals(p.totalReadOperationsSucceededCount(), 1L)
        assertEquals(p.readOperationsOutstandingCount(), 0)
      } finally { close(r); close(w) }
    }
  }
}
