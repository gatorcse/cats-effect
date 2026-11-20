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
import cats.effect.unsafe.{IORuntime, PollingSystem}
import cats.syntax.all._

import munit.FunSuite

import java.io.IOException
import java.lang.foreign.{Arena, MemorySegment, ValueLayout}

import scala.concurrent.Future
import scala.concurrent.duration._

/** Runs `IO` tests on a runtime whose polling system is [[pollingSystem]]. */
abstract class FfmRuntimeSuite extends FunSuite {

  def pollingSystem: PollingSystem = FfmEpollSystem()

  private[this] var _runtime: IORuntime = _
  implicit def runtime: IORuntime = _runtime

  override def beforeAll(): Unit = {
    _runtime = IORuntime.builder().setPollingSystem(pollingSystem).build()
  }

  override def afterAll(): Unit = _runtime.shutdown()

  def real(name: String)(body: => IO[Any]): Unit =
    test(name)(IO.defer(body).timeout(20.seconds).unsafeToFuture(): Future[Any])
}

/** Raw fd helpers shared by the suites. */
object Fds {

  def check(op: String)(rtn: Long): Long =
    if (rtn < 0) throw new IOException(s"$op failed: errno ${Libc.errno(Libc.capture())}")
    else rtn

  /** A non-blocking pipe as (readFd, writeFd). */
  def pipeUnsafe(): (Int, Int) = {
    val arena = Arena.ofConfined()
    try {
      val fds = arena.allocate(ValueLayout.JAVA_INT, 2L)
      check("pipe2")(Libc.pipe2(Libc.capture(), fds, Libc.O_NONBLOCK | Libc.O_CLOEXEC).toLong)
      (fds.getAtIndex(ValueLayout.JAVA_INT, 0L), fds.getAtIndex(ValueLayout.JAVA_INT, 1L))
    } finally arena.close()
  }

  def pipe: Resource[IO, (Int, Int)] =
    Resource.make(IO(pipeUnsafe())) {
      case (r, w) => IO { Libc.close(Libc.capture(), r); Libc.close(Libc.capture(), w); () }
    }

  def close(fd: Int): Unit = { Libc.close(Libc.capture(), fd); () }

  /** Non-blocking read; Left(()) on EAGAIN, Right(-1) at EOF. */
  def tryRead(fd: Int, buf: Array[Byte], off: Int, len: Int): Either[Unit, Int] = {
    val cap = Libc.capture()
    val n = Libc.readHeap(cap, fd, MemorySegment.ofArray(buf).asSlice(off.toLong), len.toLong)
    if (n > 0) Right(n.toInt)
    else if (n == 0) Right(-1)
    else {
      val err = Libc.errno(cap)
      if (err == Libc.EAGAIN) Left(())
      else throw new IOException(s"read failed: errno $err")
    }
  }

  /** Non-blocking write; Left(()) on EAGAIN. */
  def tryWrite(fd: Int, buf: Array[Byte], off: Int, len: Int): Either[Unit, Int] = {
    val cap = Libc.capture()
    val n = Libc.writeHeap(cap, fd, MemorySegment.ofArray(buf).asSlice(off.toLong), len.toLong)
    if (n >= 0) Right(n.toInt)
    else {
      val err = Libc.errno(cap)
      if (err == Libc.EAGAIN) Left(())
      else throw new IOException(s"write failed: errno $err")
    }
  }

  def writeUnsafe(fd: Int, bytes: Array[Byte]): Unit = {
    tryWrite(fd, bytes, 0, bytes.length).fold(_ => sys.error("pipe full"), _ => ())
  }

  final class Pipe(
      val readFd: Int,
      val writeFd: Int,
      val readHandle: FileDescriptorPollHandle,
      val writeHandle: FileDescriptorPollHandle) {

    def read(buf: Array[Byte], off: Int, len: Int): IO[Int] =
      readHandle.pollReadRec(())(_ => IO(tryRead(readFd, buf, off, len)))

    def write(buf: Array[Byte], off: Int, len: Int): IO[Int] =
      writeHandle.pollWriteRec(())(_ => IO(tryWrite(writeFd, buf, off, len)))
  }

  def registeredPipe: Resource[IO, Pipe] =
    pipe.flatMap {
      case (r, w) =>
        Resource.eval(FileDescriptorPoller.get).flatMap { poller =>
          (poller.registerFileDescriptor(r, true, false), poller.registerFileDescriptor(w, false, true))
            .mapN(new Pipe(r, w, _, _))
        }
    }
}
