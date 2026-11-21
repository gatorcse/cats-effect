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

import scala.scalanative.libc.signal.{signal, SIG_IGN}
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.errno._
import scala.scalanative.posix.fcntl._
import scala.scalanative.posix.signal.SIGPIPE
import scala.scalanative.posix.string._
import scala.scalanative.posix.sys.socket
import scala.scalanative.posix.unistd
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

import java.io.IOException

/**
 * [[PollerTestPlatform]] over `@extern` (Linux epoll; the runtime contract also runs on
 * kqueue).
 */
trait NativePollerTestPlatform extends PollerTestPlatform {

  // the JVM ignores SIGPIPE; Native would die on a write to a closed pipe instead of seeing EPIPE
  signal(SIGPIPE, SIG_IGN)

  private[this] def fail(op: String): Nothing =
    throw new IOException(s"$op failed: ${fromCString(strerror(errno))}")

  def pipe(): (Int, Int) = {
    val fds = stackalloc[CInt](2)
    if (unistd.pipe(fds) != 0) fail("pipe")
    List(fds(0), fds(1)).foreach { fd =>
      if (fcntl(fd, F_SETFL, O_NONBLOCK) != 0) fail("fcntl")
      if (fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) fail("fcntl")
    }
    (fds(0), fds(1))
  }

  def close(fd: Int): Unit = { unistd.close(fd); () }

  def tryRead(fd: Int, buf: Array[Byte], off: Int, len: Int): Either[Unit, Int] = {
    val n = unistd.read(fd, buf.atUnsafe(off), len.toCSize)
    if (n > 0) Right(n)
    else if (n == 0) Right(-1)
    else if (errno == EAGAIN || errno == EWOULDBLOCK) Left(())
    else fail("read")
  }

  def tryWrite(fd: Int, buf: Array[Byte], off: Int, len: Int): Either[Unit, Int] = {
    val n = unistd.write(fd, buf.atUnsafe(off), len.toCSize)
    if (n >= 0) Right(n)
    else if (errno == EAGAIN || errno == EWOULDBLOCK) Left(())
    else fail("write")
  }

  def unconnectedTcpSocket(): Int = {
    val fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    if (fd < 0) fail("socket")
    if (fcntl(fd, F_SETFL, O_NONBLOCK) != 0) fail("fcntl")
    fd
  }
}

trait NativeEpollTestBackend extends PollerTestPlatform {
  def backendName: String = "epoll"
  def backendSupported: Boolean = LinktimeInfo.isLinux
  def nanosecondTimeoutsAvailable: Boolean =
    if (LinktimeInfo.isLinux) NativeEpollSyscalls.epollPwait2Available else false
  def makeBackend(maxEvents: Int, nanosecondTimeouts: Boolean): EventLoopBackend =
    if (LinktimeInfo.isLinux)
      new EpollBackend(new NativeEpollSyscalls(maxEvents, nanosecondTimeouts))
    else throw new UnsupportedOperationException("epoll is Linux-only")
  def pollingSystem(): PollingSystem { type Poller <: EventLoop.Poller } =
    if (LinktimeInfo.isLinux) EpollSystem
    else throw new UnsupportedOperationException("epoll is Linux-only")
  def runtimePollingSystem: Option[PollingSystem] =
    if (LinktimeInfo.isLinux) Some(EpollSystem) else None
}

trait NativeKqueueTestBackend extends PollerTestPlatform {
  def backendName: String = "kqueue"
  def backendSupported: Boolean = LinktimeInfo.isMac
  def nanosecondTimeoutsAvailable: Boolean = true
  def makeBackend(maxEvents: Int, nanosecondTimeouts: Boolean): EventLoopBackend =
    if (LinktimeInfo.isMac) new KqueueBackend(new NativeKqueueSyscalls(maxEvents))
    else throw new UnsupportedOperationException("kqueue is macOS-only here")
  def pollingSystem(): PollingSystem { type Poller <: EventLoop.Poller } =
    if (LinktimeInfo.isMac) KqueueSystem
    else throw new UnsupportedOperationException("kqueue is macOS-only here")
  def runtimePollingSystem: Option[PollingSystem] =
    if (LinktimeInfo.isMac) Some(KqueueSystem) else None
}

class NativeEpollPollerSuite
    extends EventLoopPollerContract
    with NativePollerTestPlatform
    with NativeEpollTestBackend

class NativeEpollFileDescriptorPollerSuite
    extends FileDescriptorPollerContract
    with NativePollerTestPlatform
    with NativeEpollTestBackend

class NativeKqueuePollerSuite
    extends EventLoopPollerContract
    with NativePollerTestPlatform
    with NativeKqueueTestBackend

class NativeKqueueFileDescriptorPollerSuite
    extends FileDescriptorPollerContract
    with NativePollerTestPlatform
    with NativeKqueueTestBackend
