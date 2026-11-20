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

import cats.syntax.all._

/**
 * JVM counterpart of the Scala Native `cats.effect.FileDescriptorPoller`: readiness-based
 * polling of raw file descriptors. Installed by [[EpollSystem]].
 */
trait FileDescriptorPoller {

  /**
   * Registers a non-blocking file descriptor with the poller and monitors read- and/or
   * write-ready events (edge-triggered). The descriptor is deregistered when the resource is
   * released; closing the descriptor itself remains the caller's responsibility.
   */
  def registerFileDescriptor(
      fileDescriptor: Int,
      monitorReadReady: Boolean,
      monitorWriteReady: Boolean
  ): Resource[IO, FileDescriptorPollHandle]

}

object FileDescriptorPoller {
  def find: IO[Option[FileDescriptorPoller]] =
    IO.pollers.map(_.collectFirst { case poller: FileDescriptorPoller => poller })

  def get: IO[FileDescriptorPoller] = find.flatMap(
    _.liftTo[IO](new RuntimeException("No FileDescriptorPoller installed in this IORuntime"))
  )
}

trait FileDescriptorPollHandle {

  /**
   * Repeatedly runs `f` until it returns a `Right`, suspending the fiber until the descriptor
   * becomes read-ready whenever `f` returns a `Left` (i.e. the operation hit `EAGAIN`).
   */
  def pollReadRec[A, B](a: A)(f: A => IO[Either[A, B]]): IO[B]

  /** As [[pollReadRec]] but for write-readiness. */
  def pollWriteRec[A, B](a: A)(f: A => IO[Either[A, B]]): IO[B]

  /**
   * Experimental, lower level: the number of read-ready edges observed so far. Together with
   * [[awaitReadReady]] this lets a caller that knows the descriptor is drained (e.g. a read
   * returned fewer bytes than requested) skip the `EAGAIN` read and go straight to waiting,
   * like Netty's epoll transport. Single reader only.
   */
  def readReadyCount: Int

  /** Suspends until the read-ready count differs from `seen`, returning the new count. */
  def awaitReadReady(seen: Int): IO[Int]

}
