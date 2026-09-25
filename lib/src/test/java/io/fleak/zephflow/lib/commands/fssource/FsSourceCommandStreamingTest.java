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
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandStreamingTest {

  @BeforeEach
  void setUp() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void tearDown() {
    FsBackendRegistry.unregister("file");
  }

  private record Run(List<Object> values, List<Object> raws, int acceptCalls) {}

  private static Run run(Path dir, String encodingType, Map<String, Object> extraConfig)
      throws Exception {
    Map<String, Object> rawConfig = new HashMap<>();
    rawConfig.put("backend", "file");
    rawConfig.put("root", dir.toUri().toString());
    rawConfig.put("encodingType", encodingType);
    rawConfig.putAll(extraConfig);

    List<RecordFleakData> emitted = new ArrayList<>();
    int[] acceptCalls = {0};
    SourceEventAcceptor acceptor =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> records) {
            acceptCalls[0]++;
            emitted.addAll(records);
          }

          @Override
          public void terminate() {}
        };
    FsSourceCommand command = new FsSourceCommand("n", JobContext.builder().build());
    command.parseAndValidateArg(rawConfig);
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    command.execute("u", acceptor);
    return new Run(
        emitted.stream().map(record -> record.unwrap().get("v")).toList(),
        emitted.stream().map(record -> record.unwrap().get("__raw__")).toList(),
        acceptCalls[0]);
  }

  @Test
  void aLineOrientedFileIsEmittedInSeveralChunks(@TempDir Path dir) throws Exception {
    StringBuilder payload = new StringBuilder();
    for (int value = 0; value < 500; value++) {
      payload.append("{\"v\":").append(value).append("}\n");
    }
    Files.writeString(dir.resolve("a.log"), payload.toString());

    Run run = run(dir, "JSON_OBJECT_LINE", Map.of("chunkSizeBytes", 256));

    assertEquals(500, run.values().size(), "every record is emitted exactly once");
    // JSON integers unwrap as Long (NumberPrimitiveFleakData.unwrap()), not Integer.
    assertEquals(0L, run.values().getFirst());
    assertEquals(499L, run.values().getLast());
    assertTrue(
        run.acceptCalls() > 1, "a 500-line file at a 256-byte chunk must stream, not buffer");
  }

  @Test
  void aLineOrientedFileStreamsEvenWhenItIsBiggerThanMaxFileBytes(@TempDir Path dir)
      throws Exception {
    StringBuilder payload = new StringBuilder();
    for (int value = 0; value < 500; value++) {
      payload.append("{\"v\":").append(value).append("}\n");
    }
    Files.writeString(dir.resolve("a.log"), payload.toString());

    Run run = run(dir, "JSON_OBJECT_LINE", Map.of("chunkSizeBytes", 256, "maxFileBytes", 1024L));

    assertEquals(500, run.values().size(), "maxFileBytes bounds a chunk, not a streamed file");
  }

  @Test
  void aWholeDocumentFileOverTheCapIsSkippedRatherThanBuffered(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("a.json"), "{\"v\":\"" + "x".repeat(500) + "\"}");

    Run run = run(dir, "JSON_OBJECT", Map.of("maxFileBytes", 64L));

    assertEquals(List.of(), run.values(), "the file is over the cap, so nothing is emitted");
  }

  @Test
  void aStringLineFileStreamsEvenWhenItIsBiggerThanMaxFileBytes(@TempDir Path dir)
      throws Exception {
    StringBuilder payload = new StringBuilder();
    for (int value = 0; value < 500; value++) {
      payload.append("line-").append(value).append("\n");
    }
    Files.writeString(dir.resolve("a.log"), payload.toString());

    Run run = run(dir, "STRING_LINE", Map.of("chunkSizeBytes", 256, "maxFileBytes", 1024L));

    assertEquals(500, run.raws().size(), "STRING_LINE is line-delimited, so it streams");
    assertEquals("line-0", run.raws().getFirst());
    assertEquals("line-499", run.raws().getLast());
    assertTrue(
        run.acceptCalls() > 1, "a 500-line file at a 256-byte chunk must stream, not buffer");
  }

  @Test
  void aJsonArrayFileStreamsEvenWhenItIsBiggerThanMaxFileBytes(@TempDir Path dir) throws Exception {
    StringBuilder payload = new StringBuilder("[");
    for (int value = 0; value < 5000; value++) {
      payload.append(value == 0 ? "" : ",").append("{\"v\":").append(value).append("}");
    }
    payload.append("]");
    Files.writeString(dir.resolve("a.json"), payload.toString());

    Run run = run(dir, "JSON_ARRAY", Map.of("chunkSizeBytes", 256, "maxFileBytes", 1024L));

    assertEquals(5000, run.values().size(), "maxFileBytes bounds one element, not the array");
    assertEquals(4999L, run.values().getLast());
    assertTrue(run.acceptCalls() > 1, "elements are handed downstream in batches, not all at once");
  }

  @Test
  void aCsvFileStreamsEvenWhenItIsBiggerThanMaxFileBytes(@TempDir Path dir) throws Exception {
    StringBuilder payload = new StringBuilder("v,note\n");
    for (int value = 0; value < 5000; value++) {
      payload.append(value).append(",\"row\n").append(value).append("\"\n");
    }
    Files.writeString(dir.resolve("a.csv"), payload.toString());

    Run run = run(dir, "CSV", Map.of("chunkSizeBytes", 256, "maxFileBytes", 1024L));

    assertEquals(5000, run.values().size(), "maxFileBytes bounds one row, not the file");
    assertEquals("4999", run.values().getLast());
    assertTrue(run.acceptCalls() > 1, "rows are handed downstream in batches, not all at once");
  }

  @Test
  void aSingleJsonArrayElementOverTheCapDropsTheRestOfTheFile(@TempDir Path dir) throws Exception {
    String hugeElement = "{\"v\":\"" + "x".repeat(512 * 1024) + "\"}";
    Files.writeString(dir.resolve("a.json"), "[{\"v\":\"first\"}," + hugeElement + "]");

    Run run = run(dir, "JSON_ARRAY", Map.of("chunkSizeBytes", 64, "maxFileBytes", 1024L));

    assertEquals(
        List.of("first"),
        run.values(),
        "one element past the cap is too large to buffer; the elements before it stand");
  }

  @Test
  void aGzippedLineOrientedFileIsDecompressedWhileStreaming(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("a.log.gz");
    try (java.util.zip.GZIPOutputStream gzipOutputStream =
        new java.util.zip.GZIPOutputStream(Files.newOutputStream(file))) {
      gzipOutputStream.write("{\"v\":1}\n{\"v\":2}\n".getBytes(StandardCharsets.UTF_8));
    }

    Run run = run(dir, "JSON_OBJECT_LINE", Map.of());

    // JSON integers unwrap as Long (NumberPrimitiveFleakData.unwrap()), not Integer.
    assertEquals(List.of(1L, 2L), run.values());
  }

  @Test
  void anEmptyLineOrientedFileCompletesWithoutThrowing(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("a.log"), "");

    Run run = run(dir, "JSON_OBJECT_LINE", Map.of());

    assertEquals(0, run.values().size(), "an empty file legitimately has nothing to emit");
  }

  @Test
  void anAllBlankLinesFileCompletesWithoutThrowing(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("a.log"), "\n\n  \n");

    Run run = run(dir, "JSON_OBJECT_LINE", Map.of());

    assertEquals(0, run.values().size(), "blank lines are dropped, not errors");
  }
}
