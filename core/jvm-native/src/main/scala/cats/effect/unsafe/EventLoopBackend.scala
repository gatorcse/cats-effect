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

/**
 * One OS readiness API (epoll, kqueue) as the [[EventLoop]] uses it: an edge-triggered interest
 * set keyed by a 64-bit id, a batch of ready events, and a wake-up source registered under
 * [[EventLoopBackend.WakeupId]]. An instance owns its OS descriptors and native memory.
 *
 * Unless a method says "any thread", it is called only by the poller's current owner thread.
 */
private[effect] trait EventLoopBackend {

  /**
   * For error messages and `toString`, e.g. "epoll".
   */
  def name: String

  /**
   * The size of the event batch.
   */
  def maxEvents: Int

  /**
   * Adds `fd`, edge-triggered, for the given directions, tagged with `id`. Returns 0 or the
   * errno; on failure nothing stays registered. A direction of an fd that this backend already
   * has registered fails with EEXIST. (epoll is stricter: any second registration of the fd
   * fails, while kqueue allows a read-only and a write-only handle on the same fd.)
   */
  def add(fd: Int, reads: Boolean, writes: Boolean, id: Long): Int

  /**
   * Removes `fd` for the directions it was added with; errors are ignored (the fd may already
   * be closed). Any thread.
   */
  def remove(fd: Int, reads: Boolean, writes: Boolean): Unit

  /**
   * Waits for events into the batch. `nanos` is -1 (forever), 0 (don't block) or a positive
   * timeout. Returns the event count, or `-errno`.
   */
  def poll(nanos: Long): Int

  /**
   * The id of ready event `i` from the last [[poll]].
   */
  def eventId(i: Int): Long

  /**
   * The directions ready event `i` wakes, as [[EventLoopBackend.Readable]] and
   * [[EventLoopBackend.Writable]] bits. Hang-ups and errors wake both, so that waiters see them
   * in their next syscall.
   */
  def eventReadiness(i: Int): Int

  /**
   * Signals the wake-up source. Any thread, but never after [[close]].
   */
  def wake(): Unit

  /**
   * Resets the wake-up source after its event was seen.
   */
  def clearWake(): Unit

  /**
   * Handles ready event `i` whose id is negative but not [[EventLoopBackend.WakeupId]]. Returns
   * true if it completed a callback.
   */
  def dispatchOther(i: Int, id: Long): Boolean

  /**
   * Whether events of [[dispatchOther]]'s kind are outstanding, so the owner must poll.
   */
  def hasOther: Boolean

  /**
   * Closes the descriptors and frees native memory. Called once, last.
   */
  def close(): Unit
}

private[effect] object EventLoopBackend {

  /**
   * The id of the wake-up source. Never a handle id.
   */
  final val WakeupId = -1L

  final val Readable = 1
  final val Writable = 2

  /**
   * The same on Linux, macOS and the BSDs.
   */
  final val EINTR = 4
}
