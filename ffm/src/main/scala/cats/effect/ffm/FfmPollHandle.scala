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

/**
 * A [[cats.effect.FileDescriptorPollHandle FileDescriptorPollHandle]] with an extra,
 * lower-level readiness protocol for callers that already guarantee a single reader.
 *
 * With edge-triggered polling, a read that returns fewer bytes than requested means the
 * descriptor is drained. Such a caller can then skip the read that would fail with `EAGAIN` and
 * wait for the next edge directly, as Netty's epoll transport does:
 *
 * {{{
 * val before = handle.readReadyCount
 * val n = read(...)              // short read
 * ...
 * handle.awaitReadReady(before)  // next time: wait for a fresh edge first
 * }}}
 *
 * Phase 0 measured this at one fewer syscall per message. The counters only ever increase.
 */
trait FfmPollHandle extends FileDescriptorPollHandle {

  /** The number of read-ready notifications delivered so far. */
  def readReadyCount: Int

  /** The number of write-ready notifications delivered so far. */
  def writeReadyCount: Int

  /**
   * Suspends until the read-ready count differs from `seen`, returning the new count. Not
   * synchronized with [[pollReadRec]]: the caller must ensure a single reader.
   */
  def awaitReadReady(seen: Int): IO[Int]

  /** As [[awaitReadReady]] for write readiness. */
  def awaitWriteReady(seen: Int): IO[Int]
}
