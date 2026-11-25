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
package io.fleak.zephflow.runner;

import io.fleak.zephflow.lib.utils.JsonUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/** Rewrites the run stats file on an interval; each write replaces the file atomically. */
@Slf4j
final class RunStatsFileWriter implements AutoCloseable {

  static final Duration INTERVAL = Duration.ofSeconds(5);

  private final Path file;
  private final Path tempFile;
  private final Supplier<RunStats> stats;
  private ScheduledExecutorService scheduler;

  RunStatsFileWriter(Path file, Supplier<RunStats> stats) {
    this.file = file;
    this.tempFile = file.resolveSibling(file.getFileName() + ".tmp");
    this.stats = stats;
  }

  static RunStatsFileWriter start(Path file, Supplier<RunStats> stats) {
    return start(file, stats, INTERVAL);
  }

  static RunStatsFileWriter start(Path file, Supplier<RunStats> stats, Duration interval) {
    RunStatsFileWriter writer = new RunStatsFileWriter(file, stats);
    writer.write();
    writer.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "run-stats-writer");
              thread.setDaemon(true);
              return thread;
            });
    writer.scheduler.scheduleWithFixedDelay(
        writer::write, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    return writer;
  }

  void write() {
    try {
      Files.writeString(tempFile, JsonUtils.toJsonString(stats.get()));
      Files.move(
          tempFile, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      log.warn("Failed to write run stats to {}", file, e);
    }
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }
}
