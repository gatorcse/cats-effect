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


/**
 * Raw libc bindings shared by every FFM polling system, via the Foreign Function & Memory API
 * (JDK 25+, which accepts {@code critical} together with {@code captureCallState}; JDK 22
 * rejects that combination).
 *
 * <p>This layer is written in Java on purpose: {@code MethodHandle.invokeExact} is
 * signature-polymorphic and Java guarantees the exact call-site type the JIT needs to inline the
 * downcall stub. Scala code calls the typed static wrappers.
 *
 * <p>Naming of the wrappers:
 *
 * <ul>
 *   <li>Every call that can fail takes a {@code cap} segment first. {@code errno} is captured
 *       into it immediately after the call ({@link Linker.Option#captureCallState}), before the
 *       JVM can clobber it; read it with {@link #errno(MemorySegment)}. Get a segment for the
 *       current thread from {@link #capture()}, or allocate one per poller with {@link
 *       #allocateCapture(Arena)}.
 *   <li>Non-blocking calls are {@link Linker.Option#critical(boolean) critical}: no thread-state
 *       transition. Calls that can block ({@link #epollWait}, {@link #epollPwait2}) never are,
 *       because a critical call holds up safepoints.
 *   <li>{@code read}/{@code write} have a {@code Heap} variant that accepts a heap segment (e.g.
 *       {@code MemorySegment.ofArray(bytes)}) with no copy, using critical heap access.
 * </ul>
 */
public final class Libc {

  private Libc() {}

  // --- platform --------------------------------------------------------------------------------

  public static final String OS = System.getProperty("os.name", "").toLowerCase();
  public static final String ARCH = System.getProperty("os.arch", "").toLowerCase();

  public static final boolean IS_LINUX = OS.startsWith("linux");
  public static final boolean IS_X86_64 = ARCH.equals("amd64") || ARCH.equals("x86_64");
  public static final boolean IS_AARCH64 = ARCH.equals("aarch64") || ARCH.equals("arm64");

  // --- errno values (Linux generic) ------------------------------------------------------------

  public static final int EPERM = 1;
  public static final int ENOENT = 2;
  public static final int EINTR = 4;
  public static final int EBADF = 9;
  public static final int EAGAIN = 11;
  public static final int EINVAL = 22;
  public static final int ENOSYS = 38;

  // --- flags -----------------------------------------------------------------------------------

  public static final int O_NONBLOCK = 0x800;
  public static final int O_CLOEXEC = 0x80000;
  public static final int F_GETFL = 3;
  public static final int F_SETFL = 4;

  public static final int AF_UNIX = 1;
  public static final int AF_INET = 2;
  public static final int SOCK_STREAM = 1;
  public static final int SOCK_NONBLOCK = O_NONBLOCK;
  public static final int SOCK_CLOEXEC = O_CLOEXEC;
  public static final int EPIPE = 32;

  // --- linker plumbing -------------------------------------------------------------------------

  static final Linker LINKER = Linker.nativeLinker();

  public static final StructLayout CAPTURE_LAYOUT = Linker.Option.captureStateLayout();
  private static final VarHandle ERRNO_HANDLE =
      CAPTURE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("errno"));

  static final Linker.Option CAPTURE = Linker.Option.captureCallState("errno");
  static final Linker.Option CRITICAL = Linker.Option.critical(false);
  private static final Linker.Option CRITICAL_HEAP = Linker.Option.critical(true);

  static Optional<MemorySegment> find(String name) {
    return LINKER.defaultLookup().find(name);
  }

  static MethodHandle bind(String name, FunctionDescriptor fd, Linker.Option... opts) {
    MemorySegment addr =
        find(name).orElseThrow(() -> new UnsatisfiedLinkError("libc symbol not found: " + name));
    return LINKER.downcallHandle(addr, fd, opts);
  }

  static MethodHandle bindOptional(
      String name, FunctionDescriptor fd, Linker.Option... opts) {
    return find(name).map(addr -> LINKER.downcallHandle(addr, fd, opts)).orElse(null);
  }

  /** Allocates a segment to receive captured call state. */
  public static MemorySegment allocateCapture(Arena arena) {
    return arena.allocate(CAPTURE_LAYOUT);
  }

  private static final ThreadLocal<MemorySegment> CAPTURE_TL =
      ThreadLocal.withInitial(() -> allocateCapture(Arena.ofAuto()));

  /** The current thread's capture segment, allocated on first use and freed with the thread. */
  public static MemorySegment capture() {
    return CAPTURE_TL.get();
  }

  public static int errno(MemorySegment capture) {
    return (int) ERRNO_HANDLE.get(capture, 0L);
  }

  // --- descriptors -----------------------------------------------------------------------------

  static final FunctionDescriptor I_I = FunctionDescriptor.of(JAVA_INT, JAVA_INT);
  static final FunctionDescriptor I_II =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT);
  static final FunctionDescriptor I_III =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT);
  static final FunctionDescriptor I_IIIP =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS);
  static final FunctionDescriptor I_IPII =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT);
  static final FunctionDescriptor I_IPIPP =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, ADDRESS);
  static final FunctionDescriptor L_IPL =
      FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG);
  static final FunctionDescriptor I_PI = FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT);
  static final FunctionDescriptor I_IIIP_SP =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS);

  // --- handles ---------------------------------------------------------------------------------

  private static final MethodHandle close = bind("close", I_I, CAPTURE);
  private static final MethodHandle fcntl =
      bind("fcntl", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT), CAPTURE);
  private static final MethodHandle pipe2 = bind("pipe2", I_PI, CAPTURE);
  private static final MethodHandle socketpair = bind("socketpair", I_IIIP_SP, CAPTURE);
  private static final MethodHandle socket = bind("socket", I_III, CAPTURE);
  private static final MethodHandle shutdown = bind("shutdown", I_II, CRITICAL, CAPTURE);

  private static final MethodHandle read = bind("read", L_IPL, CRITICAL, CAPTURE);
  private static final MethodHandle readHeap = bind("read", L_IPL, CRITICAL_HEAP, CAPTURE);
  private static final MethodHandle readBlocking = bind("read", L_IPL, CAPTURE);
  private static final MethodHandle write = bind("write", L_IPL, CRITICAL, CAPTURE);
  private static final MethodHandle writeHeap = bind("write", L_IPL, CRITICAL_HEAP, CAPTURE);

  // --- wrappers --------------------------------------------------------------------------------

  public static int close(MemorySegment cap, int fd) {
    try {
      return (int) close.invokeExact(cap, fd);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int fcntl(MemorySegment cap, int fd, int cmd, int arg) {
    try {
      return (int) fcntl.invokeExact(cap, fd, cmd, arg);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** {@code fds} must hold two ints. */
  public static int pipe2(MemorySegment cap, MemorySegment fds, int flags) {
    try {
      return (int) pipe2.invokeExact(cap, fds, flags);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** {@code fds} must hold two ints. */
  public static int socketpair(
      MemorySegment cap, int domain, int type, int protocol, MemorySegment fds) {
    try {
      return (int) socketpair.invokeExact(cap, domain, type, protocol, fds);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int socket(MemorySegment cap, int domain, int type, int protocol) {
    try {
      return (int) socket.invokeExact(cap, domain, type, protocol);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int shutdown(MemorySegment cap, int fd, int how) {
    try {
      return (int) shutdown.invokeExact(cap, fd, how);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** Non-blocking read into a native segment. */
  public static long read(MemorySegment cap, int fd, MemorySegment buf, long len) {
    try {
      return (long) read.invokeExact(cap, fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** Non-blocking read into a native or heap segment, with no copy. */
  public static long readHeap(MemorySegment cap, int fd, MemorySegment buf, long len) {
    try {
      return (long) readHeap.invokeExact(cap, fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** A read that may block (not critical). */
  public static long readBlocking(MemorySegment cap, int fd, MemorySegment buf, long len) {
    try {
      return (long) readBlocking.invokeExact(cap, fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** Non-blocking write from a native segment. */
  public static long write(MemorySegment cap, int fd, MemorySegment buf, long len) {
    try {
      return (long) write.invokeExact(cap, fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** Non-blocking write from a native or heap segment, with no copy. */
  public static long writeHeap(MemorySegment cap, int fd, MemorySegment buf, long len) {
    try {
      return (long) writeHeap.invokeExact(cap, fd, buf, len);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  static RuntimeException rethrow(Throwable t) {
    if (t instanceof RuntimeException) return (RuntimeException) t;
    if (t instanceof Error) throw (Error) t;
    return new RuntimeException(t);
  }
}
