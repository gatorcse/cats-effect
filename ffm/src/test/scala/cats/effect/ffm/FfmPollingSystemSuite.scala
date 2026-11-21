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

import munit.FunSuite

class FfmPollingSystemSuite extends FunSuite {

  test("FFM is available on this platform") {
    assertEquals(FfmPollingSystem.availability, Right(()))
  }

  test("select() returns epoll on Linux and kqueue on macOS") {
    val selected = FfmPollingSystem.select()
    if (internal.Libc.IS_LINUX) assert(selected.exists(_.isInstanceOf[FfmEpollSystem]), selected)
    else assert(selected.exists(_.isInstanceOf[FfmKqueueSystem]), selected)
  }

  test("epoll_pwait2 probe does not throw") {
    assume(internal.Libc.IS_LINUX, "epoll is Linux-only")
    FfmEpollSystem.epollPwait2Available
  }
}
