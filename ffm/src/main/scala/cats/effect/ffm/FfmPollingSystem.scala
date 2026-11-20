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

import cats.effect.unsafe.{PollingSystem, SelectorSystem}

/**
 * Picks the best FFM polling system for this platform, falling back to the default
 * `SelectorSystem` when it cannot be used:
 *
 * {{{
 * object Main extends IOApp.Simple {
 *   override protected def pollingSystem = cats.effect.ffm.FfmPollingSystem()
 *   def run = ...
 * }
 * }}}
 *
 * The JVM needs native access for this module, e.g. `--enable-native-access=ALL-UNNAMED` (or
 * `Enable-Native-Access: ALL-UNNAMED` in an executable jar's manifest). Without it, JDK 22-23
 * work but warn and later JDKs may deny access, so it is detected up front.
 */
object FfmPollingSystem {

  /**
   * Why the FFM backend could not be used.
   */
  final case class Unavailable(reason: String)

  /**
   * The FFM polling system for this platform, or `SelectorSystem()` if there is none. When
   * falling back, the reason is printed to stderr once unless `-Dcats.effect.ffm.quiet=true`.
   */
  def apply(): PollingSystem =
    select() match {
      case Right(system) => system
      case Left(Unavailable(reason)) =>
        if (!java.lang.Boolean.getBoolean("cats.effect.ffm.quiet"))
          System.err.println(s"[cats-effect-ffm] falling back to SelectorSystem: $reason")
        SelectorSystem()
    }

  /**
   * The FFM polling system for this platform, or why there is none. Never throws.
   */
  def select(): Either[Unavailable, PollingSystem] =
    availability.flatMap { _ =>
      try Right(FfmEpollSystem())
      catch {
        case t: Throwable => Left(Unavailable(s"epoll initialization failed: $t"))
      }
    }

  /**
   * Whether this JVM and platform can run an FFM polling system, and if not why.
   */
  lazy val availability: Either[Unavailable, Unit] = {
    val os = System.getProperty("os.name", "")
    val arch = System.getProperty("os.arch", "")
    val supportedArch = Set("amd64", "x86_64", "aarch64", "arm64")
    val feature = Runtime.version().feature()

    if (feature < 22)
      Left(Unavailable(s"JDK $feature; the FFM API needs JDK 22+"))
    else if (!os.toLowerCase.startsWith("linux"))
      Left(Unavailable(s"no FFM polling system for $os yet (epoll needs Linux)"))
    else if (!supportedArch(arch.toLowerCase))
      Left(Unavailable(s"unsupported architecture $arch"))
    else if (!nativeAccessEnabled)
      Left(
        Unavailable("native access is not enabled for this module; run with " +
          "--enable-native-access=ALL-UNNAMED (or the module's name)"))
    else
      try {
        // forces linking of every binding; fails fast on a missing symbol
        val cap = internal.Libc.capture()
        val fd = internal.Epoll.epollCreate1(cap, internal.Epoll.EPOLL_CLOEXEC)
        if (fd < 0)
          Left(Unavailable(s"epoll_create1 failed: errno ${internal.Libc.errno(cap)}"))
        else {
          internal.Libc.close(cap, fd)
          Right(())
        }
      } catch {
        case t: Throwable => Left(Unavailable(s"FFM bindings failed to load: $t"))
      }
  }

  /**
   * `Module.isNativeAccessEnabled` (JDK 22+). On JDK 22-23 restricted methods are allowed with
   * a warning even when this is false, but we treat it as a requirement so behaviour does not
   * change silently on newer JDKs.
   */
  private def nativeAccessEnabled: Boolean =
    try getClass.getModule.isNativeAccessEnabled()
    catch { case _: Throwable => false }
}
