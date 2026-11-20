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

package cats.effect.ffm.benchmarks;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * Phase 0 spike bindings (benchmarks only) via the Foreign Function & Memory API (JDK 25+).
 *
 * <p>Every handle is a {@code static final} so the JIT can constant-fold it and inline the
 * downcall stub. Bindings come in up to three flavours:
 *
 * <ul>
 *   <li><b>plain</b>: a normal downcall. The thread transitions to native state and back, so a
 *       safepoint can happen during the call. Required for anything that may block (e.g.
 *       {@code epoll_wait} with a timeout).
 *   <li><b>{@code E} suffix</b>: a normal downcall that also captures {@code errno} into a
 *       caller-supplied segment immediately after the call, before the JVM can clobber it.
 *   <li><b>{@code C} suffix</b>: a {@link Linker.Option#critical(boolean) critical} downcall. No
 *       thread-state transition, so it is cheaper, but it must never block and it delays
 *       safepoints for its duration. The heap-access variants accept heap segments (e.g. wrapping a
 *       {@code byte[]}) directly, with no copy, like JNI's {@code GetPrimitiveArrayCritical}.
 * </ul>
 */
public final class BenchLibc {

  private BenchLibc() {}

  // --- constants (x86_64 / aarch64 Linux) ---------------------------------------------------

  public static final int EPOLL_CTL_ADD = 1;
  public static final int EPOLL_CTL_DEL = 2;
  public static final int EPOLL_CTL_MOD = 3;

  public static final int EPOLLIN = 0x001;
  public static final int EPOLLOUT = 0x004;
  public static final int EPOLLERR = 0x008;
  public static final int EPOLLHUP = 0x010;
  public static final int EPOLLRDHUP = 0x2000;
  public static final int EPOLLET = 1 << 31;

  public static final int EFD_NONBLOCK = 0x800;
  public static final int EFD_CLOEXEC = 0x80000;

  public static final int AF_INET = 2;
  public static final int SOCK_STREAM = 1;
  public static final int SOCK_NONBLOCK = 0x800;
  public static final int SOCK_CLOEXEC = 0x80000;
  public static final int SOL_SOCKET = 1;
  public static final int SO_REUSEADDR = 2;
  public static final int SO_REUSEPORT = 15;
  public static final int IPPROTO_TCP = 6;
  public static final int TCP_NODELAY = 1;

  public static final int EINTR = 4;
  public static final int EAGAIN = 11;
  public static final int EINPROGRESS = 115;

  // --- layouts --------------------------------------------------------------------------------

  /**
   * {@code struct epoll_event} is packed on x86_64 (12 bytes) and naturally aligned (16 bytes)
   * elsewhere.
   */
  public static final boolean X86_64 = "amd64".equals(System.getProperty("os.arch"))
      || "x86_64".equals(System.getProperty("os.arch"));

  public static final long EPOLL_EVENT_SIZE = X86_64 ? 12 : 16;
  public static final long EPOLL_EVENT_DATA_OFFSET = X86_64 ? 4 : 8;

  /** Unaligned access for the packed x86_64 epoll_event. */
  public static final ValueLayout.OfLong JAVA_LONG_UNALIGNED = JAVA_LONG.withByteAlignment(1);

  public static final StructLayout SOCKADDR_IN = MemoryLayout.structLayout(
      ValueLayout.JAVA_SHORT.withName("sin_family"),
      ValueLayout.JAVA_SHORT.withName("sin_port"),
      JAVA_INT.withName("sin_addr"),
      MemoryLayout.sequenceLayout(8, ValueLayout.JAVA_BYTE).withName("sin_zero"));

  // --- linker plumbing --------------------------------------------------------------------------

  private static final Linker LINKER = Linker.nativeLinker();
  private static final SymbolLookup LIBC = LINKER.defaultLookup();

  public static final StructLayout CAPTURE_LAYOUT = Linker.Option.captureStateLayout();
  private static final VarHandle ERRNO_HANDLE =
      CAPTURE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("errno"));

  private static final Linker.Option CAPTURE = Linker.Option.captureCallState("errno");
  private static final Linker.Option CRITICAL = Linker.Option.critical(false);
  private static final Linker.Option CRITICAL_HEAP = Linker.Option.critical(true);

  private static MethodHandle bind(String name, FunctionDescriptor fd, Linker.Option... opts) {
    MemorySegment addr = LIBC
        .find(name)
        .orElseThrow(() -> new UnsatisfiedLinkError("libc symbol not found: " + name));
    return LINKER.downcallHandle(addr, fd, opts);
  }

  /** Allocates a segment to receive captured call state; one per thread/poller. */
  public static MemorySegment allocateCapture(Arena arena) {
    return arena.allocate(CAPTURE_LAYOUT);
  }

  public static int errno(MemorySegment capture) {
    return (int) ERRNO_HANDLE.get(capture, 0L);
  }

  // --- descriptors ---------------------------------------------------------------------------

  private static final FunctionDescriptor I_I = FunctionDescriptor.of(JAVA_INT, JAVA_INT);
  private static final FunctionDescriptor I_II =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT);
  private static final FunctionDescriptor I_III =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT);
  private static final FunctionDescriptor I_IIIP =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS);
  private static final FunctionDescriptor I_IPII =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT);
  private static final FunctionDescriptor L_IPL =
      FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG);
  private static final FunctionDescriptor I_IPI =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT);
  private static final FunctionDescriptor I_IPPI =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS, JAVA_INT);
  private static final FunctionDescriptor I_IIIPI =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT);
  private static final FunctionDescriptor I_V = FunctionDescriptor.of(JAVA_INT);

  // --- handles -------------------------------------------------------------------------------

  // trivial, non-syscall function: isolates pure downcall overhead
  public static final MethodHandle abs = bind("abs", I_I);
  public static final MethodHandle absC = bind("abs", I_I, CRITICAL);
  public static final MethodHandle absE = bind("abs", I_I, CAPTURE);
  public static final MethodHandle absCE = bind("abs", I_I, CRITICAL, CAPTURE);

  // cheapest real syscall
  public static final MethodHandle getppid = bind("getppid", I_V);
  public static final MethodHandle getppidC = bind("getppid", I_V, CRITICAL);

  public static final MethodHandle epoll_create1 = bind("epoll_create1", I_I);
  public static final MethodHandle epoll_ctl = bind("epoll_ctl", I_IIIP);
  public static final MethodHandle epoll_ctlE = bind("epoll_ctl", I_IIIP, CAPTURE);
  /** May block: never critical. */
  public static final MethodHandle epoll_waitE = bind("epoll_wait", I_IPII, CAPTURE);
  /** Only for timeout == 0. */
  public static final MethodHandle epoll_waitCE =
      bind("epoll_wait", I_IPII, CRITICAL, CAPTURE);

  public static final MethodHandle eventfd = bind("eventfd", I_II);
  public static final MethodHandle close = bind("close", I_I);

  public static final MethodHandle read = bind("read", L_IPL);
  public static final MethodHandle readE = bind("read", L_IPL, CAPTURE);
  public static final MethodHandle readCE = bind("read", L_IPL, CRITICAL, CAPTURE);
  public static final MethodHandle readCHE = bind("read", L_IPL, CRITICAL_HEAP, CAPTURE);
  public static final MethodHandle write = bind("write", L_IPL);
  public static final MethodHandle writeE = bind("write", L_IPL, CAPTURE);
  public static final MethodHandle writeCE = bind("write", L_IPL, CRITICAL, CAPTURE);
  public static final MethodHandle writeCHE = bind("write", L_IPL, CRITICAL_HEAP, CAPTURE);

  public static final MethodHandle pipe2 =
      bind("pipe2", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT));
  public static final MethodHandle socket = bind("socket", I_III);
  public static final MethodHandle bind = bind("bind", I_IPI);
  public static final MethodHandle listen = bind("listen", I_II);
  public static final MethodHandle connectE = bind("connect", I_IPI, CAPTURE);
  public static final MethodHandle accept4E = bind("accept4", I_IPPI, CAPTURE);
  public static final MethodHandle accept4CE = bind("accept4", I_IPPI, CRITICAL, CAPTURE);
  public static final MethodHandle setsockopt = bind("setsockopt", I_IIIPI);
  public static final MethodHandle shutdown = bind("shutdown", I_II);

  // --- typed wrappers (keep invokeExact call sites monomorphic and Scala-friendly) -------------

  public static int abs(int x) {
    try {
      return (int) abs.invokeExact(x);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int absC(int x) {
    try {
      return (int) absC.invokeExact(x);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int absE(MemorySegment cap, int x) {
    try {
      return (int) absE.invokeExact(cap, x);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int absCE(MemorySegment cap, int x) {
    try {
      return (int) absCE.invokeExact(cap, x);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int getppid() {
    try {
      return (int) getppid.invokeExact();
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int getppidC() {
    try {
      return (int) getppidC.invokeExact();
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int epollCreate1(int flags) {
    try {
      return (int) epoll_create1.invokeExact(flags);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int epollCtl(MemorySegment cap, int epfd, int op, int fd, MemorySegment ev) {
    try {
      return (int) epoll_ctlE.invokeExact(cap, epfd, op, fd, ev);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int epollCtlDel(int epfd, int fd) {
    try {
      return (int) epoll_ctl.invokeExact(epfd, EPOLL_CTL_DEL, fd, MemorySegment.NULL);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int epollWait(
      MemorySegment cap, int epfd, MemorySegment events, int max, int timeout) {
    try {
      return (int) epoll_waitE.invokeExact(cap, epfd, events, max, timeout);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int epollWaitNow(MemorySegment cap, int epfd, MemorySegment events, int max) {
    try {
      return (int) epoll_waitCE.invokeExact(cap, epfd, events, max, 0);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int eventfd(int init, int flags) {
    try {
      return (int) eventfd.invokeExact(init, flags);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int close(int fd) {
    try {
      return (int) close.invokeExact(fd);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static long read(int fd, MemorySegment buf, long len) {
    try {
      return (long) read.invokeExact(fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static long readE(MemorySegment cap, int fd, MemorySegment buf, long len) {
    try {
      return (long) readE.invokeExact(cap, fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static long readCE(MemorySegment cap, int fd, MemorySegment buf, long len) {
    try {
      return (long) readCE.invokeExact(cap, fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** {@code buf} may be a heap segment (e.g. {@code MemorySegment.ofArray(bytes)}). */
  public static long readCHE(MemorySegment cap, int fd, MemorySegment buf, long len) {
    try {
      return (long) readCHE.invokeExact(cap, fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static long write(int fd, MemorySegment buf, long len) {
    try {
      return (long) write.invokeExact(fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static long writeE(MemorySegment cap, int fd, MemorySegment buf, long len) {
    try {
      return (long) writeE.invokeExact(cap, fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static long writeCE(MemorySegment cap, int fd, MemorySegment buf, long len) {
    try {
      return (long) writeCE.invokeExact(cap, fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** {@code buf} may be a heap segment (e.g. {@code MemorySegment.ofArray(bytes)}). */
  public static long writeCHE(MemorySegment cap, int fd, MemorySegment buf, long len) {
    try {
      return (long) writeCHE.invokeExact(cap, fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** {@code fds} must hold two ints. */
  public static int pipe2(MemorySegment fds, int flags) {
    try {
      return (int) pipe2.invokeExact(fds, flags);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int socket(int domain, int type, int proto) {
    try {
      return (int) socket.invokeExact(domain, type, proto);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int bind(int fd, MemorySegment addr, int len) {
    try {
      return (int) bind.invokeExact(fd, addr, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int listen(int fd, int backlog) {
    try {
      return (int) listen.invokeExact(fd, backlog);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int connect(MemorySegment cap, int fd, MemorySegment addr, int len) {
    try {
      return (int) connectE.invokeExact(cap, fd, addr, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int accept4(MemorySegment cap, int fd, int flags) {
    try {
      return (int) accept4CE.invokeExact(cap, fd, MemorySegment.NULL, MemorySegment.NULL, flags);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int setsockopt(int fd, int level, int name, MemorySegment val, int len) {
    try {
      return (int) setsockopt.invokeExact(fd, level, name, val, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int shutdown(int fd, int how) {
    try {
      return (int) shutdown.invokeExact(fd, how);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  private static RuntimeException rethrow(Throwable t) {
    if (t instanceof RuntimeException) return (RuntimeException) t;
    if (t instanceof Error) throw (Error) t;
    return new RuntimeException(t);
  }
}
