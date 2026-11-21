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

import cats.effect.ffm.internal.{Epoll, Libc}

import munit.FunSuite

/**
 * Checks on the FFM bindings themselves; the poller contract is shared with Native.
 */
class FfmBindingSuite extends FunSuite {

  test("epoll_event matches the platform ABI") {
    assume(Libc.IS_LINUX, "epoll is Linux-only")
    if (Libc.IS_X86_64) {
      assertEquals(Epoll.EPOLL_EVENT_SIZE, 12L)
      assertEquals(Epoll.EPOLL_EVENT_DATA_OFFSET, 4L)
    } else {
      assertEquals(Epoll.EPOLL_EVENT_SIZE, 16L)
      assertEquals(Epoll.EPOLL_EVENT_DATA_OFFSET, 8L)
    }
    assertEquals(Epoll.TIMESPEC.byteSize(), 16L)
  }

  test("errno is captured") {
    val cap = Libc.capture()
    assertEquals(Libc.close(cap, -1), -1)
    assertEquals(Libc.errno(cap), Libc.EBADF)
  }
}
