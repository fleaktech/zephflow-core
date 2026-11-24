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

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RunStatsFileWriterTest {

  @TempDir Path dir;

  @Test
  void writesTheStatsAsJsonWithUnknownBacklogAsNull() throws Exception {
    Path file = dir.resolve("stats.json");
    RunStats stats =
        new RunStats(
            List.of(
                new RunStats.SourceStats("kafka", 150L, "records", 6, 1_000L),
                new RunStats.SourceStats("pubsub", null, "records", null, null)),
            RunStats.BufferState.PENDING,
            RunStats.DrainState.RUNNING,
            false);

    try (RunStatsFileWriter ignored = RunStatsFileWriter.start(file, () -> stats)) {
      assertEquals(
          Map.of(
              "sources",
              List.of(
                  Map.of(
                      "nodeId",
                      "kafka",
                      "backlog",
                      150,
                      "backlogUnit",
                      "records",
                      "partitionCount",
                      6,
                      "consumedRecords",
                      1000),
                  nullBacklog("pubsub")),
              "bufferState",
              "PENDING",
              "drainState",
              "RUNNING",
              "supportsDrain",
              false),
          read(file));
    }
  }

  @Test
  void rewritesTheFileOnEveryInterval() throws Exception {
    Path file = dir.resolve("stats.json");
    AtomicLong backlog = new AtomicLong();

    try (RunStatsFileWriter ignored =
        RunStatsFileWriter.start(
            file, () -> stats(backlog.incrementAndGet()), Duration.ofMillis(20))) {
      long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      while (backlog.get() < 3 && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
    }

    assertTrue(backlog.get() >= 3, "stats were collected " + backlog.get() + " times");
    assertTrue(firstBacklog(read(file)) >= 2);
  }

  @Test
  void readersNeverSeeAHalfWrittenFile() throws Exception {
    Path file = dir.resolve("stats.json");
    int sourceCount = 20_000;
    List<RunStats.SourceStats> manySources =
        LongStream.range(0, sourceCount)
            .mapToObj(i -> new RunStats.SourceStats("source-" + i, i, "records", 6, i))
            .toList();
    RunStats big =
        new RunStats(manySources, RunStats.BufferState.EMPTY, RunStats.DrainState.RUNNING, false);
    RunStatsFileWriter writer = new RunStatsFileWriter(file, () -> big);
    writer.write();
    AtomicBoolean writing = new AtomicBoolean(true);
    List<Throwable> readFailures = new ArrayList<>();
    Thread reader =
        new Thread(
            () -> {
              while (writing.get()) {
                try {
                  Map<String, Object> json = read(file);
                  assertEquals(sourceCount, ((List<?>) json.get("sources")).size());
                } catch (Throwable t) {
                  readFailures.add(t);
                }
              }
            });

    reader.start();
    for (int i = 0; i < 100; i++) {
      writer.write();
    }
    writing.set(false);
    reader.join();

    assertEquals(List.of(), readFailures.stream().limit(1).toList());
    try (var files = Files.list(dir)) {
      assertEquals(List.of(file), files.toList());
    }
  }

  private static RunStats stats(long backlog) {
    return new RunStats(
        List.of(new RunStats.SourceStats("kafka", backlog, "records", 6, backlog)),
        RunStats.BufferState.EMPTY,
        RunStats.DrainState.RUNNING,
        false);
  }

  private static Map<String, Object> nullBacklog(String nodeId) {
    Map<String, Object> source = new java.util.HashMap<>();
    source.put("nodeId", nodeId);
    source.put("backlog", null);
    source.put("backlogUnit", "records");
    source.put("partitionCount", null);
    source.put("consumedRecords", null);
    return source;
  }

  private static long firstBacklog(Map<String, Object> json) {
    List<?> sources = (List<?>) json.get("sources");
    return ((Number) ((Map<?, ?>) sources.getFirst()).get("backlog")).longValue();
  }

  private static Map<String, Object> read(Path file) throws Exception {
    return fromJsonString(Files.readString(file), new TypeReference<>() {});
  }
}
