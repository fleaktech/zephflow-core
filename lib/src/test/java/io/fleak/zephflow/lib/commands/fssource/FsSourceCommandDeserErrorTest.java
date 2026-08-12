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

import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.FleakGauge;
import io.fleak.zephflow.api.metric.FleakStopWatch;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A malformed record must not cost the whole file: the records that parse are emitted, and the ones
 * that don't are counted and quarantined instead of vanishing into a log line.
 */
class FsSourceCommandDeserErrorTest {

  private final List<DeadLetter> deadLetters = new ArrayList<>();
  private final Map<String, AtomicLong> counters = new HashMap<>();

  @BeforeEach
  void registerBackend() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
  }

  /** Two well-formed array lines, one truncated line, one more well-formed line. */
  private static final String PAYLOAD =
      """
      [{"device_id":"sensor-119","value":69.4},{"device_id":"sensor-111","value":64.9}]
      [{"device_id":"sensor-102","value":62.7}]
      [{"device_id":"sensor-103","value":73.4},
      [{"device_id":"sensor-104","value":64.4}]
      """;

  private List<RecordFleakData> run(Path tempDir, boolean withDlq) throws Exception {
    Map<String, Object> rawConfig =
        Map.of(
            "backend", "file",
            "root", tempDir.toUri().toString(),
            "fileNameRegex", "evt_(?<ts>\\d+)\\.jsonl",
            "encodingType", "JSON_OBJECT_LINE");
    List<RecordFleakData> emitted = new ArrayList<>();
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> records) {
            emitted.addAll(records);
          }

          @Override
          public void terminate() {}
        };

    FsSourceCommand command = new FsSourceCommand("node-1", JobContext.builder().build());
    command.parseAndValidateArg(rawConfig);
    command.initialize(new RecordingMetricClientProvider());
    if (withDlq) {
      // The real writer is built from JobContext.dlqConfig; swap in a capturing one so the test
      // doesn't need a cloud backend.
      ((FsSourceExecutionContext) command.getExecutionContext()).dlqWriter =
          new CapturingDlqWriter();
    }
    command.execute("user", out);
    return emitted;
  }

  @Test
  void badLine_doesNotDiscardTheGoodRecordsInTheSameFile(@TempDir Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("evt_1.jsonl"), PAYLOAD);

    List<RecordFleakData> emitted = run(tempDir, true);

    assertEquals(
        List.of("sensor-119", "sensor-111", "sensor-102", "sensor-104"),
        emitted.stream().map(r -> r.unwrap().get("device_id")).toList());
  }

  @Test
  void badLine_isCountedAndWrittenToTheDlq(@TempDir Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("evt_1.jsonl"), PAYLOAD);

    run(tempDir, true);

    assertEquals(1, counters.get(METRIC_NAME_INPUT_DESER_ERR_COUNT).get());
    assertEquals(4, counters.get(METRIC_NAME_INPUT_EVENT_COUNT).get());

    assertEquals(1, deadLetters.size());
    DeadLetter deadLetter = deadLetters.getFirst();
    assertEquals(
        "[{\"device_id\":\"sensor-103\",\"value\":73.4},",
        new String(deadLetter.getValue().array(), StandardCharsets.UTF_8),
        "the raw failing line should be quarantined");
    assertEquals("node-1", deadLetter.getNodeId());
    assertTrue(
        deadLetter.getErrorMessage().contains("failed to parse json object line"),
        "error message should explain the failure, got: " + deadLetter.getErrorMessage());

    Map<String, String> metadata = new HashMap<>(deadLetter.getMetadata());
    assertTrue(
        metadata.get(METADATA_FS_SOURCE_URN).endsWith("evt_1.jsonl"),
        "should record which file the bad record came from, got: " + metadata);
    assertEquals("3", metadata.get(METADATA_FS_SOURCE_RECORD_INDEX), "should record the line");
  }

  @Test
  void partiallyBadFile_isCheckpointedSoTheGoodRecordsAreNotReEmitted(@TempDir Path tempDir)
      throws Exception {
    Files.writeString(tempDir.resolve("evt_1.jsonl"), PAYLOAD);

    // In-memory checkpoint client, so a second run in the same process resumes from the first.
    Map<String, Object> rawConfig =
        Map.of(
            "backend", "file",
            "root", tempDir.toUri().toString(),
            "fileNameRegex", "evt_(?<ts>\\d+)\\.jsonl",
            "encodingType", "JSON_OBJECT_LINE");
    FsSourceCommand command = new FsSourceCommand("node-1", JobContext.builder().build());
    command.parseAndValidateArg(rawConfig);
    command.initialize(new RecordingMetricClientProvider());
    FsSourceExecutionContext executionContext =
        (FsSourceExecutionContext) command.getExecutionContext();
    executionContext.dlqWriter = new CapturingDlqWriter();

    List<RecordFleakData> firstRun = new ArrayList<>();
    command.execute("user", acceptorInto(firstRun));
    assertEquals(4, firstRun.size());

    List<RecordFleakData> secondRun = new ArrayList<>();
    command.execute("user", acceptorInto(secondRun));
    assertTrue(secondRun.isEmpty(), "a checkpointed file must not be re-read");
  }

  @Test
  void whollyUnparseableFileWithNoDlq_isLeftForRetryRatherThanCheckpointed(@TempDir Path tempDir)
      throws Exception {
    Files.writeString(tempDir.resolve("evt_1.jsonl"), "not json at all\n");

    FsSourceCommand command = new FsSourceCommand("node-1", JobContext.builder().build());
    command.parseAndValidateArg(
        Map.of(
            "backend", "file",
            "root", tempDir.toUri().toString(),
            "fileNameRegex", "evt_(?<ts>\\d+)\\.jsonl",
            "encodingType", "JSON_OBJECT_LINE"));
    command.initialize(new RecordingMetricClientProvider());

    List<RecordFleakData> firstRun = new ArrayList<>();
    command.execute("user", acceptorInto(firstRun));
    assertTrue(firstRun.isEmpty());
    assertEquals(1, counters.get(METRIC_NAME_INPUT_DESER_ERR_COUNT).get());

    List<RecordFleakData> secondRun = new ArrayList<>();
    command.execute("user", acceptorInto(secondRun));
    assertEquals(
        2,
        counters.get(METRIC_NAME_INPUT_DESER_ERR_COUNT).get(),
        "with nowhere to quarantine it, the file should be retried rather than silently dropped");
  }

  private static SourceEventAcceptor acceptorInto(List<RecordFleakData> sink) {
    return new SourceEventAcceptor() {
      @Override
      public void accept(List<RecordFleakData> records) {
        sink.addAll(records);
      }

      @Override
      public void terminate() {}
    };
  }

  private class CapturingDlqWriter extends DlqWriter {
    @Override
    protected void doWrite(DeadLetter deadLetter) {
      deadLetters.add(deadLetter);
    }

    @Override
    public void open() {}

    @Override
    public void close() {}
  }

  private class RecordingMetricClientProvider implements MetricClientProvider {
    @Override
    public FleakCounter counter(String name, Map<String, String> tags) {
      AtomicLong count = counters.computeIfAbsent(name, k -> new AtomicLong());
      return new FleakCounter() {
        @Override
        public void increase(Map<String, String> additionalTags) {
          count.incrementAndGet();
        }

        @Override
        public void increase(long n, Map<String, String> additionalTags) {
          count.addAndGet(n);
        }
      };
    }

    @Override
    public <T> FleakGauge<T> gauge(String name, Map<String, String> tags, T monitoredValue) {
      return new MetricClientProvider.NoopMetricClientProvider().gauge(name, tags, monitoredValue);
    }

    @Override
    public FleakStopWatch stopWatch(String name, Map<String, String> tags) {
      return new MetricClientProvider.NoopMetricClientProvider().stopWatch(name, tags);
    }

    @Override
    public void close() {}
  }
}
