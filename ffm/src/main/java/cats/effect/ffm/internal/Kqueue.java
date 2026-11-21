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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;

/**
 * {@code kqueue} and {@code kevent} bindings, with the 32-byte {@code struct kevent} of 64-bit
 * macOS. Load only on macOS, or where {@link #LIBRARY_PROPERTY} names a library that provides
 * them (libkqueue on Linux, for tests).
 */
public final class Kqueue {
  private Kqueue() {}

  /**
   * A system property naming a shared library to take {@code kqueue} and {@code kevent} from,
   * instead of libc. For running the kqueue backend on Linux against libkqueue in tests.
   */
  public static final String LIBRARY_PROPERTY = "cats.effect.ffm.kqueueLibrary";

  private static final String LIBRARY = System.getProperty(LIBRARY_PROPERTY);

  /** Where the symbols come from: libc on macOS, or {@link #LIBRARY_PROPERTY}. */
  public static boolean usesLibraryOverride() {
    return LIBRARY != null;
  }

  // --- struct kevent { uintptr_t ident; int16_t filter; uint16_t flags; uint32_t fflags;
  //                     intptr_t data; void *udata; } ----------------------------------------
  public static final long KEVENT_SIZE = 32L;
  public static final long KEVENT_IDENT = 0L;
  public static final long KEVENT_FILTER = 8L;
  public static final long KEVENT_FLAGS = 10L;
  public static final long KEVENT_FFLAGS = 12L;
  public static final long KEVENT_DATA = 16L;
  public static final long KEVENT_UDATA = 24L;

  /** {@code EVFILT_USER}: -10 on macOS, -11 on FreeBSD and in libkqueue. */
  public static final short EVFILT_USER = (Libc.IS_MAC && LIBRARY == null) ? (short) -10 : (short) -11;

  // --- handles ---------------------------------------------------------------------------------

  private static final SymbolLookup LOOKUP =
      LIBRARY == null
          ? Libc.LINKER.defaultLookup()
          : SymbolLookup.libraryLookup(Path.of(LIBRARY), Arena.global());

  private static MethodHandle bind(String name, FunctionDescriptor fd, java.lang.foreign.Linker.Option... opts) {
    MemorySegment addr =
        LOOKUP.find(name).orElseThrow(() -> new UnsatisfiedLinkError("symbol not found: " + name));
    return Libc.LINKER.downcallHandle(addr, fd, opts);
  }

  private static final FunctionDescriptor KEVENT =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS);

  private static final MethodHandle kqueue =
      bind("kqueue", FunctionDescriptor.of(JAVA_INT), Libc.CAPTURE);
  private static final MethodHandle kevent = bind("kevent", KEVENT, Libc.CAPTURE);
  private static final MethodHandle kevent_now =
      bind("kevent", KEVENT, Libc.CRITICAL, Libc.CAPTURE);

  // --- wrappers --------------------------------------------------------------------------------

  public static int kqueue(MemorySegment cap) {
    try {
      return (int) kqueue.invokeExact(cap);
    } catch (Throwable t) {
      throw Libc.rethrow(t);
    }
  }

  /** A {@code kevent} that may block: {@code timeout} is NULL (forever) or positive. */
  public static int kevent(
      MemorySegment cap,
      int kq,
      MemorySegment changes,
      int nchanges,
      MemorySegment events,
      int nevents,
      MemorySegment timeout) {
    try {
      return (int) kevent.invokeExact(cap, kq, changes, nchanges, events, nevents, timeout);
    } catch (Throwable t) {
      throw Libc.rethrow(t);
    }
  }

  /** A {@code kevent} with a zero {@code timeout}, as a critical downcall. */
  public static int keventNow(
      MemorySegment cap,
      int kq,
      MemorySegment changes,
      int nchanges,
      MemorySegment events,
      int nevents,
      MemorySegment zeroTimeout) {
    try {
      return (int) kevent_now.invokeExact(cap, kq, changes, nchanges, events, nevents, zeroTimeout);
    } catch (Throwable t) {
      throw Libc.rethrow(t);
    }
  }
}
