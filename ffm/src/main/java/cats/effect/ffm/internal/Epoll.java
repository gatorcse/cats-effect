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

package cats.effect.ffm.internal;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.util.Optional;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;


/** Linux {@code epoll} and {@code eventfd} bindings. Load only on Linux. */
public final class Epoll {

  private Epoll() {}

  // --- epoll -----------------------------------------------------------------------------------

  public static final int EPOLL_CLOEXEC = Libc.O_CLOEXEC;
  public static final int EPOLL_CTL_ADD = 1;
  public static final int EPOLL_CTL_DEL = 2;
  public static final int EPOLL_CTL_MOD = 3;

  public static final int EPOLLIN = 0x001;
  public static final int EPOLLPRI = 0x002;
  public static final int EPOLLOUT = 0x004;
  public static final int EPOLLERR = 0x008;
  public static final int EPOLLHUP = 0x010;
  public static final int EPOLLRDHUP = 0x2000;
  public static final int EPOLLET = 1 << 31;

  public static final int EFD_NONBLOCK = Libc.O_NONBLOCK;
  public static final int EFD_CLOEXEC = Libc.O_CLOEXEC;

  /**
   * {@code struct epoll_event { uint32_t events; epoll_data_t data; }}. glibc declares it
   * {@code __attribute__((packed))} on x86_64 only, so it is 12 bytes there and 16 (with 4 bytes
   * of padding) elsewhere.
   */
  public static final StructLayout EPOLL_EVENT = Libc.IS_X86_64
      ? MemoryLayout.structLayout(
          JAVA_INT.withName("events"), JAVA_LONG.withByteAlignment(1).withName("data"))
      : MemoryLayout.structLayout(
          JAVA_INT.withName("events"),
          MemoryLayout.paddingLayout(4),
          JAVA_LONG.withName("data"));

  public static final long EPOLL_EVENT_SIZE = EPOLL_EVENT.byteSize();
  public static final long EPOLL_EVENT_EVENTS_OFFSET =
      EPOLL_EVENT.byteOffset(MemoryLayout.PathElement.groupElement("events"));
  public static final long EPOLL_EVENT_DATA_OFFSET =
      EPOLL_EVENT.byteOffset(MemoryLayout.PathElement.groupElement("data"));

  /** Unaligned long, for the packed x86_64 {@code epoll_event.data}. */
  public static final ValueLayout.OfLong JAVA_LONG_UNALIGNED = JAVA_LONG.withByteAlignment(1);

  /** {@code struct timespec { time_t tv_sec; long tv_nsec; }} on LP64. */
  public static final StructLayout TIMESPEC =
      MemoryLayout.structLayout(JAVA_LONG.withName("tv_sec"), JAVA_LONG.withName("tv_nsec"));

  private static final MethodHandle epoll_create1 = Libc.bind("epoll_create1", Libc.I_I, Libc.CAPTURE);
  private static final MethodHandle epoll_ctl = Libc.bind("epoll_ctl", Libc.I_IIIP, Libc.CRITICAL, Libc.CAPTURE);
  private static final MethodHandle epoll_wait = Libc.bind("epoll_wait", Libc.I_IPII, Libc.CAPTURE);
  private static final MethodHandle epoll_wait_now =
      Libc.bind("epoll_wait", Libc.I_IPII, Libc.CRITICAL, Libc.CAPTURE);
  /** glibc 2.35+; {@code null} when the symbol is missing. */
  private static final MethodHandle epoll_pwait2 = Libc.bindOptional("epoll_pwait2", Libc.I_IPIPP, Libc.CAPTURE);

  private static final MethodHandle eventfd = Libc.bind("eventfd", Libc.I_II, Libc.CAPTURE);

  public static int epollCreate1(MemorySegment cap, int flags) {
    try {
      return (int) epoll_create1.invokeExact(cap, flags);
    } catch (Throwable t) {
      throw Libc.rethrow(t);
    }
  }

  public static int epollCtl(
      MemorySegment cap, int epfd, int op, int fd, MemorySegment event) {
    try {
      return (int) epoll_ctl.invokeExact(cap, epfd, op, fd, event);
    } catch (Throwable t) {
      throw Libc.rethrow(t);
    }
  }

  /** Blocking wait; {@code timeoutMillis} is -1 (forever) or positive. */
  public static int epollWait(
      MemorySegment cap, int epfd, MemorySegment events, int maxEvents, int timeoutMillis) {
    try {
      return (int) epoll_wait.invokeExact(cap, epfd, events, maxEvents, timeoutMillis);
    } catch (Throwable t) {
      throw Libc.rethrow(t);
    }
  }

  /** Non-blocking poll (timeout 0), as a critical downcall. */
  public static int epollWaitNow(MemorySegment cap, int epfd, MemorySegment events, int maxEvents) {
    try {
      return (int) epoll_wait_now.invokeExact(cap, epfd, events, maxEvents, 0);
    } catch (Throwable t) {
      throw Libc.rethrow(t);
    }
  }

  public static boolean hasEpollPwait2Symbol() {
    return epoll_pwait2 != null;
  }

  /** Blocking wait with a {@code struct timespec} timeout; requires {@link #hasEpollPwait2Symbol}. */
  public static int epollPwait2(
      MemorySegment cap, int epfd, MemorySegment events, int maxEvents, MemorySegment timespec) {
    try {
      return (int)
          epoll_pwait2.invokeExact(cap, epfd, events, maxEvents, timespec, MemorySegment.NULL);
    } catch (Throwable t) {
      throw Libc.rethrow(t);
    }
  }

  public static int eventfd(MemorySegment cap, int initval, int flags) {
    try {
      return (int) eventfd.invokeExact(cap, initval, flags);
    } catch (Throwable t) {
      throw Libc.rethrow(t);
    }
  }

}
