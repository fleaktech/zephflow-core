/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Drains resources before a bounded execution can truthfully report terminal completion. */
public final class ExecutorShutdown {
  private ExecutorShutdown() {}

  public static void awaitExit(ExecutorService executor, boolean abort) {
    if (executor == null) return;
    if (abort) {
      executor
          .shutdownNow()
          .forEach(
              task -> {
                if (task instanceof Future<?> future) future.cancel(false);
              });
    } else {
      executor.shutdown();
    }
    boolean interrupted = Thread.interrupted();
    try {
      while (!executor.isTerminated()) {
        try {
          executor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException interruption) {
          interrupted = true;
          if (abort) executor.shutdownNow();
        }
      }
    } finally {
      if (interrupted) Thread.currentThread().interrupt();
    }
  }
}
