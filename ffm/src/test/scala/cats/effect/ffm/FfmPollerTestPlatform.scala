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

import cats.effect.ffm.internal.{FfmEpollSyscalls, FfmKqueueSyscalls, Libc}
import cats.effect.unsafe.{
  EpollBackend,
  EventLoop,
  EventLoopBackend,
  KqueueBackend,
  PollerTestPlatform,
  PollingSystem
}

import java.io.IOException
import java.lang.foreign.{Arena, MemorySegment, ValueLayout}

/**
 * Raw fd operations through FFM; the backend under test comes from [[FfmEpollTestBackend]] or
 * [[FfmKqueueTestBackend]].
 */
trait FfmPollerTestPlatform extends PollerTestPlatform {

  private[this] def fail(op: String, err: Int): Nothing =
    throw new IOException(s"$op failed: errno $err")

  private[this] def setFlags(fd: Int): Unit = {
    val cap = Libc.capture()
    val fl = Libc.fcntl(cap, fd, Libc.F_GETFL, 0)
    if (fl < 0) fail("fcntl(F_GETFL)", Libc.errno(cap))
    if (Libc.fcntl(cap, fd, Libc.F_SETFL, fl | Libc.O_NONBLOCK) < 0)
      fail("fcntl(F_SETFL)", Libc.errno(cap))
    if (Libc.fcntl(cap, fd, Libc.F_SETFD, Libc.FD_CLOEXEC) < 0)
      fail("fcntl(F_SETFD)", Libc.errno(cap))
  }

  def pipe(): (Int, Int) = {
    val arena = Arena.ofConfined()
    try {
      val fds = arena.allocate(ValueLayout.JAVA_INT, 2L)
      val cap = Libc.capture()
      if (Libc.pipe(cap, fds) < 0) fail("pipe", Libc.errno(cap))
      val r = fds.getAtIndex(ValueLayout.JAVA_INT, 0L)
      val w = fds.getAtIndex(ValueLayout.JAVA_INT, 1L)
      setFlags(r)
      setFlags(w)
      (r, w)
    } finally arena.close()
  }
  def close(fd: Int): Unit = { Libc.close(Libc.capture(), fd); () }

  def tryRead(fd: Int, buf: Array[Byte], off: Int, len: Int): Either[Unit, Int] = {
    val cap = Libc.capture()
    val n = Libc.readHeap(cap, fd, MemorySegment.ofArray(buf).asSlice(off.toLong), len.toLong)
    if (n > 0) Right(n.toInt)
    else if (n == 0) Right(-1)
    else {
      val err = Libc.errno(cap)
      if (err == Libc.EAGAIN) Left(()) else fail("read", err)
    }
  }

  def tryWrite(fd: Int, buf: Array[Byte], off: Int, len: Int): Either[Unit, Int] = {
    val cap = Libc.capture()
    val n = Libc.writeHeap(cap, fd, MemorySegment.ofArray(buf).asSlice(off.toLong), len.toLong)
    if (n >= 0) Right(n.toInt)
    else {
      val err = Libc.errno(cap)
      if (err == Libc.EAGAIN) Left(()) else fail("write", err)
    }
  }

  def unconnectedTcpSocket(): Int = {
    val cap = Libc.capture()
    val fd = Libc.socket(cap, Libc.AF_INET, Libc.SOCK_STREAM, 0)
    if (fd < 0) fail("socket", Libc.errno(cap))
    setFlags(fd)
    fd
  }
}

trait FfmEpollTestBackend extends PollerTestPlatform {
  def backendName: String = "epoll"
  def backendSupported: Boolean = Libc.IS_LINUX
  def nanosecondTimeoutsAvailable: Boolean = FfmEpollSystem.epollPwait2Available
  def makeBackend(maxEvents: Int, nanosecondTimeouts: Boolean): EventLoopBackend =
    new EpollBackend(new FfmEpollSyscalls(maxEvents, nanosecondTimeouts))
  def pollingSystem(): PollingSystem { type Poller <: EventLoop.Poller } = FfmEpollSystem()
  def runtimePollingSystem: Option[PollingSystem] = Some(FfmEpollSystem())
}

/**
 * kqueue on macOS, or on Linux against libkqueue when the build sets
 * `cats.effect.ffm.kqueueLibrary` (only if libkqueue is installed).
 */
trait FfmKqueueTestBackend extends PollerTestPlatform {
  def backendName: String = "kqueue"
  def backendSupported: Boolean = {
    val supported = Libc.IS_MAC || System.getProperty("cats.effect.ffm.kqueueLibrary") != null
    // CI sets this where libkqueue is installed, so that the suites can't skip silently
    if (!supported && sys.env.contains("REQUIRE_KQUEUE"))
      throw new AssertionError("REQUIRE_KQUEUE is set but kqueue (or libkqueue) is unavailable")
    supported
  }
  def nanosecondTimeoutsAvailable: Boolean = true
  override def emulated: Boolean = !Libc.IS_MAC
  def makeBackend(maxEvents: Int, nanosecondTimeouts: Boolean): EventLoopBackend =
    new KqueueBackend(new FfmKqueueSyscalls(maxEvents))
  def pollingSystem(): PollingSystem { type Poller <: EventLoop.Poller } = FfmKqueueSystem()
  def runtimePollingSystem: Option[PollingSystem] = Some(FfmKqueueSystem())
}

class FfmEpollPollerSuite
    extends cats.effect.unsafe.EventLoopPollerContract
    with FfmPollerTestPlatform
    with FfmEpollTestBackend

class FfmEpollFileDescriptorPollerSuite
    extends cats.effect.unsafe.FileDescriptorPollerContract
    with FfmPollerTestPlatform
    with FfmEpollTestBackend

class FfmKqueuePollerSuite
    extends cats.effect.unsafe.EventLoopPollerContract
    with FfmPollerTestPlatform
    with FfmKqueueTestBackend

class FfmKqueueFileDescriptorPollerSuite
    extends cats.effect.unsafe.FileDescriptorPollerContract
    with FfmPollerTestPlatform
    with FfmKqueueTestBackend
