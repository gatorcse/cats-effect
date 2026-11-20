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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

/**
 * Prints process-wide allocation and GC counters once per second to stderr, as
 * {@code STAT <epochMillis> <allocatedBytes> <gcCount> <gcMillis>}. The benchmark harness diffs two
 * samples bracketing the measurement window.
 */
public final class AllocReporter {
  private AllocReporter() {}

  public static void start() {
    com.sun.management.ThreadMXBean tmx =
        (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    Thread t = new Thread(
        () -> {
          while (true) {
            long gcCount = 0, gcMs = 0;
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
              gcCount += Math.max(0, gc.getCollectionCount());
              gcMs += Math.max(0, gc.getCollectionTime());
            }
            System.err.println("STAT " + System.currentTimeMillis() + " "
                + tmx.getTotalThreadAllocatedBytes() + " " + gcCount + " " + gcMs);
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              return;
            }
          }
        },
        "alloc-reporter");
    t.setDaemon(true);
    t.start();
  }
}
