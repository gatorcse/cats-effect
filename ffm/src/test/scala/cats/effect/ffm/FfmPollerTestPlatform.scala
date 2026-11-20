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

import cats.effect.ffm.internal.{FfmEpollSyscalls, Libc}
import cats.effect.unsafe.{EpollPollingSystem, EpollSyscalls, PollerTestPlatform, PollingSystem}

import java.io.IOException
import java.lang.foreign.{Arena, MemorySegment, ValueLayout}

/** [[PollerTestPlatform]] over FFM. */
trait FfmPollerTestPlatform extends PollerTestPlatform {

  def epollSupported: Boolean = Libc.IS_LINUX

  def epollPwait2Available: Boolean = FfmEpollSystem.epollPwait2Available

  def makeSyscalls(maxEvents: Int, nanosecondTimeouts: Boolean): EpollSyscalls =
    new FfmEpollSyscalls(maxEvents, nanosecondTimeouts)

  def epollSystem(): EpollPollingSystem = FfmEpollSystem()

  def runtimePollingSystem: Option[PollingSystem] = Some(FfmEpollSystem())

  private[this] def fail(op: String, err: Int): Nothing =
    throw new IOException(s"$op failed: errno $err")

  def pipe(): (Int, Int) = {
    val arena = Arena.ofConfined()
    try {
      val fds = arena.allocate(ValueLayout.JAVA_INT, 2L)
      val cap = Libc.capture()
      if (Libc.pipe2(cap, fds, Libc.O_NONBLOCK | Libc.O_CLOEXEC) < 0)
        fail("pipe2", Libc.errno(cap))
      (fds.getAtIndex(ValueLayout.JAVA_INT, 0L), fds.getAtIndex(ValueLayout.JAVA_INT, 1L))
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
    val fd = Libc.socket(cap, Libc.AF_INET, Libc.SOCK_STREAM | Libc.SOCK_NONBLOCK, 0)
    if (fd < 0) fail("socket", Libc.errno(cap))
    fd
  }
}

class FfmEpollPollerSuite extends cats.effect.unsafe.EpollPollerContract with FfmPollerTestPlatform

class FfmFileDescriptorPollerSuite
    extends cats.effect.unsafe.FileDescriptorPollerContract
    with FfmPollerTestPlatform
