/*
 * Copyright 2017 Daniel Spiewak
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

package cats
package effect

import scala.concurrent.duration._
import scala.io.StdIn

object StdIO {

  def println(str: String): IO[Unit] = IO { Console.println(str) }

  def print[A: Show](a: A): IO[Unit] = IO { Console.print(Show[A].show(a)) }

  val readLine: IO[String] = IO { StdIn.readLine }

  // arguably, this isn't an effect, since it's a constant that comes from the runtime
  def getenv(key: String): IO[Option[String]] = IO { Option(System.getenv(key)) }

  def systemProperty(key: String): IO[Option[String]] = IO { Option(System.getProperty(key)) }

  def identityHashCode(ref: AnyRef): IO[Int] = IO { System.identityHashCode(ref) }

  val nanoTime: IO[FiniteDuration] = IO { System.nanoTime.nanos }

  val currentTimeMillis: IO[FiniteDuration] = IO { System.currentTimeMillis.millis }

  def exit(code: Int): IO[Nothing] = IO { System.exit(code); sys.error("unreachable") }

  val availableProcessors: IO[Int] = IO { Runtime.getRuntime.availableProcessors }

  def addShutdownHook(action: IO[Unit]): IO[Unit] = IO {
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run() = {
        action.unsafeRunSync()
      }
    })
  }
}