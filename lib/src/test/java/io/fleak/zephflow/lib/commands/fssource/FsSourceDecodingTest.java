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

import static io.fleak.zephflow.lib.utils.MiscUtils.METADATA_FS_SOURCE_RECORD_INDEX;
import static io.fleak.zephflow.lib.utils.MiscUtils.METADATA_FS_SOURCE_URN;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_DESER_ERR_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_SIZE_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_FILE_SKIPPED_COUNT;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.FleakGauge;
import io.fleak.zephflow.api.metric.FleakStopWatch;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * FsSource on the generic decoder: the texts, nodes, values and metadata of its DLQ entries, a read
 * failure across runs, and its log lines. What it emits per encoding is FsSourceSnapshotTest's.
 */
class FsSourceDecodingTest {

  private static final String RENAME =
      "then upload the corrected file under a new name; a name already read is not read again";
  private static final String RENAME_WITH_REGEX =
      "then upload the corrected file under a new name that matches fileNameRegex (with a ts no"
          + " older than the files already read, if the regex has one); a name already read is not"
          + " read again";
  private static final String RECORD_ACTION =
      "Action: fix this record where it is written, or the source's encodingType or decoding if"
          + " every record fails like this; the value is the record's bytes";
  private static final String RECORD_LIMIT_FROM_SETTING =
      "Action: this source sets maxRecordBytes to maxFileBytes, so raise maxFileBytes (and the"
          + " task's heap to match), or write smaller records; if records should be much smaller,"
          + " check that the source's format matches the file (a wrong format or framing, or an"
          + " unclosed csv quote, joins many records into one); ";

  /** A DLQ entry as a comparable value; the stack trace between its lines varies. */
  record Entry(
      String value,
      Map<String, String> metadata,
      String firstLine,
      String action,
      String lastLine,
      String nodeId) {}

  @TempDir Path root;
  private final List<Entry> dlq = new ArrayList<>();
  private final Map<String, Long> counters = new TreeMap<>();
  private final List<Integer> batches = new ArrayList<>();
  private FsSourceCommand command;

  @BeforeEach
  void registerBackend() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
  }

  @Test
  void aBadLineIsARecordEntryWithItsBytesAndFileRelativeIndex() throws Exception {
    run(file("a.jsonl", "{\"a\":1}\n{\"a\":\n{\"a\":3}\n"), "JSON_OBJECT_LINE", Map.of(), true);

    assertEquals(
        List.of(
            new Entry(
                "{\"a\":",
                Map.of(
                    METADATA_FS_SOURCE_URN, urn("a.jsonl"), METADATA_FS_SOURCE_RECORD_INDEX, "2"),
                "MALFORMED json record: JsonEOFException",
                RECORD_ACTION,
                "file " + urn("a.jsonl") + ", recordIndex 2, bytes 8-13",
                "source")),
        dlq);
  }

  @Test
  void aSmallBadDocumentKeepsItsBytesAndALargeOneIsSummarized() throws Exception {
    run(file("small.json", "{\"a\":"), "JSON_OBJECT", Map.of(), true);
    Files.delete(root.resolve("small.json"));
    command = null;
    run(file("large.json", "{\"a\":\"" + "x".repeat(140_000)), "JSON_OBJECT", Map.of(), true);

    assertEquals(
        List.of(
            new Entry(
                "{\"a\":",
                Map.of(METADATA_FS_SOURCE_URN, urn("small.json")),
                "MALFORMED json record: JsonEOFException",
                RECORD_ACTION,
                "file " + urn("small.json") + ", bytes 0-5",
                "source"),
            new Entry(
                "",
                Map.of(METADATA_FS_SOURCE_URN, urn("large.json")),
                "MALFORMED json file: JsonEOFException",
                "Action: check that the file is json as the source's encodingType or decoding says,"
                    + " or fix the listed records where the file is written; "
                    + RENAME,
                "file "
                    + urn("large.json")
                    + "; 1 outcome; the document (bytes 0-140006); the file ended",
                "source")),
        dlq);
  }

  @Test
  void aSmallBadXmlDocumentKeepsItsBytesAndALargeOneIsSummarized() throws Exception {
    run(file("small.xml", "<root><a>1</a>"), "XML", Map.of(), true);
    Files.delete(root.resolve("small.xml"));
    command = null;
    run(file("large.xml", "<root><a>" + "x".repeat(140_000)), "XML", Map.of(), true);

    assertEquals(
        List.of(
            List.of("<root><a>1</a>", "source", "file " + urn("small.xml") + ", bytes 0-14"),
            List.of(
                "",
                "source",
                "file "
                    + urn("large.xml")
                    + "; 1 outcome; the document (bytes 0-140009); the file ended")),
        dlq.stream().map(e -> List.of(e.value(), e.nodeId(), e.lastLine())).toList());
  }

  @Test
  void anOversizedLineIsSummarizedWithTheSettingToRaiseAndTheRestIsRead() throws Exception {
    run(
        file("f_1.jsonl", "{\"a\":1}\n{\"a\":\"" + "x".repeat(200) + "\"}\n{\"a\":3}\n"),
        "JSON_OBJECT_LINE",
        Map.of("maxFileBytes", 64L, "fileNameRegex", "f_(?<ts>\\d+)\\.jsonl"),
        true);

    assertEquals(
        List.of(
            new Entry(
                "",
                Map.of(METADATA_FS_SOURCE_URN, urn("f_1.jsonl")),
                "OVERSIZED json file: maxRecordBytes",
                RECORD_LIMIT_FROM_SETTING + RENAME_WITH_REGEX,
                "file " + urn("f_1.jsonl") + "; 1 outcome; recordIndex 2 (bytes 8-216)",
                "source")),
        dlq);
    assertEquals(List.of(2), batches);
  }

  @Test
  void withoutMaxFileBytesTheActionAsksForMoreHeap() throws Exception {
    run(
        file("a.jsonl", "{\"a\":\"" + "x".repeat(2000) + "\"}\n"),
        "JSON_OBJECT_LINE",
        Map.of(),
        true,
        128 * 1024);

    assertEquals(
        List.of(
            "Action: this source sets maxRecordBytes to 1/128 of the task's heap, so give the task"
                + " a heap of at least 128 times the record's size, or write smaller records; if"
                + " records should be much smaller, check that the source's format matches the file"
                + " (a wrong format or framing, or an unclosed csv quote, joins many records into"
                + " one); "
                + RENAME),
        dlq.stream().map(Entry::action).toList());
  }

  @Test
  void csvHeaderAndOpenQuoteFailuresEndTheFileWithASummary() throws Exception {
    run(file("dup.csv", "id,id\n1,2\n"), "CSV", Map.of(), true);
    Files.delete(root.resolve("dup.csv"));
    command = null;
    run(
        file("open.csv", "id,name\n1,x\n2,\"open" + "y".repeat(200)),
        "CSV",
        Map.of("maxFileBytes", 64L),
        true);

    assertEquals(
        List.of(
            new Entry(
                "",
                Map.of(METADATA_FS_SOURCE_URN, urn("dup.csv")),
                "MALFORMED csv file: MalformedCsvException",
                "Action: check that the file is csv as the source's encodingType or decoding says,"
                    + " or fix the listed records where the file is written; "
                    + RENAME,
                "file " + urn("dup.csv") + "; 1 outcome; the header; the file ended",
                "source"),
            new Entry(
                "",
                Map.of(METADATA_FS_SOURCE_URN, urn("open.csv")),
                "OVERSIZED csv file: maxRecordBytes",
                RECORD_LIMIT_FROM_SETTING + RENAME,
                "file "
                    + urn("open.csv")
                    + "; 1 outcome; recordIndex 2; the file ended at recordIndex 2",
                "source")),
        dlq);
  }

  @Test
  void anOversizedJsonArrayElementEndsTheFileAfterTheElementsBeforeIt() throws Exception {
    run(
        file("a.json", "[{\"a\":1},{\"long\":\"" + "x".repeat(200_000) + "\"},{\"a\":3}]"),
        "JSON_ARRAY",
        Map.of("maxFileBytes", 1000L),
        true);

    assertEquals(
        List.of(
            new Entry(
                "",
                Map.of(METADATA_FS_SOURCE_URN, urn("a.json")),
                "OVERSIZED json file: maxRecordBytes",
                RECORD_LIMIT_FROM_SETTING + RENAME,
                "file "
                    + urn("a.json")
                    + "; 1 outcome; recordIndex 2; the file ended at recordIndex 2",
                "source")),
        dlq);
    assertEquals(List.of(1), batches);
  }

  @Test
  void aCompressionFailureHoldsTheFileDropsTheOpenBatchAndIsReadAgain() throws Exception {
    StringBuilder lines = new StringBuilder();
    long emittedBytes = 0;
    for (int i = 1; i <= 1500; i++) {
      String line =
          i == 3
              ? "{bad\n"
              : i == 5 ? "{\"x\":\"" + "y".repeat(200) + "\"}\n" : "{\"i\":" + i + "}\n";
      lines.append(line);
      // the first batch holds the first 1,000 good lines, through line 1,002
      if (i <= 1002) {
        emittedBytes += line.length();
      }
    }
    long sizeThroughLastEmittedRecord = emittedBytes - 1;
    Path file =
        file(
            "a.jsonl.gz",
            concat(gzip(lines.toString()), "trailing".getBytes(StandardCharsets.UTF_8)));

    for (int run = 0; run < 2; run++) {
      dlq.clear();
      counters.clear();
      batches.clear();
      try (BoundedLogCapture logs = new BoundedLogCapture(FsFileDecoder.class)) {
        assertThrows(
            IllegalStateException.class,
            () -> run(file, "JSON_OBJECT_LINE", Map.of("maxFileBytes", 100L), true));

        assertEquals(
            List.of(
                List.of(Level.ERROR),
                List.of(
                    "fs_source skip file urn="
                        + urn("a.jsonl.gz")
                        + " due to a read failure; it is read again on every run until it is fixed."
                        + " Check whether the file was still being written when it was listed, the"
                        + " compression setting (decoding.compression, or gzip detected"
                        + " automatically for encodingType), and whether its compressed data is"
                        + " damaged or has bytes after its end, in which case rewrite the file")),
            List.of(
                logs.events().stream().map(LogEvent::getLevel).toList(),
                logs.events().stream().map(e -> e.getMessage().getFormattedMessage()).toList()));
      }
      assertEquals(List.of(1000), batches);
      assertEquals(List.of("MALFORMED json record: JsonParseException"), firstLines());
      assertEquals(
          Map.of(
              METRIC_NAME_INPUT_EVENT_COUNT,
              1000L,
              METRIC_NAME_INPUT_EVENT_SIZE_COUNT,
              sizeThroughLastEmittedRecord,
              METRIC_NAME_INPUT_DESER_ERR_COUNT,
              2L,
              METRIC_NAME_INPUT_FILE_SKIPPED_COUNT + "{skip_reason=read_error}",
              1L),
          counters);
    }
  }

  @Test
  void eachFileReadIsLoggedOnce() throws Exception {
    Path file = file("a.jsonl", "{\"a\":1}\n");

    try (BoundedLogCapture logs = new BoundedLogCapture(FsFileAccess.class)) {
      run(file, "JSON_OBJECT_LINE", Map.of(), true);

      assertEquals(
          List.of("fs_source reading urn=" + urn("a.jsonl") + " listedSizeBytes=8 in place"),
          logs.events().stream()
              .filter(e -> e.getLevel() == Level.INFO)
              .map(e -> e.getMessage().getFormattedMessage())
              .toList());
    }
  }

  @Test
  void withoutADlqEachSummaryIsLoggedOnceAndTheFileIsDone() throws Exception {
    Path file = file("a.jsonl", "{bad\n{bad\n{\"x\":\"" + "y".repeat(200) + "\"}\n{\"a\":1}\n");

    try (BoundedLogCapture logs = new BoundedLogCapture(UnitRun.class)) {
      run(file, "JSON_OBJECT_LINE", Map.of("maxFileBytes", 100L), false);
      run(file, "JSON_OBJECT_LINE", Map.of("maxFileBytes", 100L), false);

      assertEquals(
          List.of(
              "dropping failures of "
                  + urn("a.jsonl")
                  + " (no DLQ configured): MALFORMED json file:"
                  + " JsonParseException",
              "dropping failures of "
                  + urn("a.jsonl")
                  + " (no DLQ configured): OVERSIZED json file:"
                  + " maxRecordBytes"),
          logs.events().stream()
              .filter(e -> e.getLevel() == Level.ERROR)
              .map(e -> e.getMessage().getFormattedMessage().lines().findFirst().orElseThrow())
              .toList());
    }
    assertEquals(List.of(1), batches);
  }

  private void run(Path file, String encodingType, Map<String, Object> extra, boolean withDlq)
      throws Exception {
    run(file, encodingType, extra, withDlq, 0);
  }

  /** Runs the source over {@code root}; one command per test unless it is reset to null. */
  private void run(
      Path file, String encodingType, Map<String, Object> extra, boolean withDlq, long heapBytes)
      throws Exception {
    if (command == null) {
      Map<String, Object> config = new HashMap<>(extra);
      config.put("backend", "file");
      config.put("root", root.toUri().toString());
      config.put("encodingType", encodingType);
      command = new FsSourceCommand("source", JobContext.builder().build());
      command.parseAndValidateArg(config);
      command.initialize(new RecordingMetricClientProvider());
      FsSourceExecutionContext context = (FsSourceExecutionContext) command.getExecutionContext();
      if (withDlq) {
        context.dlqWriter = new CapturingDlqWriter();
      }
      if (heapBytes > 0) {
        context.heapBytes = heapBytes;
      }
    }
    command.execute(
        "user",
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> batch) {
            batches.add(batch.size());
          }

          @Override
          public void terminate() {}
        });
  }

  private List<String> firstLines() {
    return dlq.stream().map(Entry::firstLine).toList();
  }

  private Path file(String name, String content) throws IOException {
    return file(name, content.getBytes(StandardCharsets.UTF_8));
  }

  private Path file(String name, byte[] content) throws IOException {
    return Files.write(root.resolve(name), content);
  }

  private String urn(String name) {
    return root.resolve(name).toUri().toString();
  }

  private static byte[] gzip(String s) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
      gz.write(s.getBytes(StandardCharsets.UTF_8));
    }
    return out.toByteArray();
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] out = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }

  private class CapturingDlqWriter extends DlqWriter {
    @Override
    protected void doWrite(DeadLetter deadLetter) {
      List<String> lines = deadLetter.getErrorMessage().lines().toList();
      Map<String, String> metadata = new HashMap<>();
      deadLetter.getMetadata().forEach((k, v) -> metadata.put(k.toString(), v.toString()));
      dlq.add(
          new Entry(
              StandardCharsets.UTF_8.decode(deadLetter.getValue().duplicate()).toString(),
              metadata,
              lines.getFirst(),
              lines.get(1),
              lines.getLast(),
              deadLetter.getNodeId()));
    }

    @Override
    public void open() {}

    @Override
    public void close() {}
  }

  /** Counts by metric name, and for skipped files by name and reason. */
  private class RecordingMetricClientProvider implements MetricClientProvider {
    @Override
    public FleakCounter counter(String name, Map<String, String> tags) {
      return new FleakCounter() {
        @Override
        public void increase(Map<String, String> additionalTags) {
          increase(1, additionalTags);
        }

        @Override
        public void increase(long n, Map<String, String> additionalTags) {
          String key =
              name.equals(METRIC_NAME_INPUT_FILE_SKIPPED_COUNT)
                  ? name + new TreeMap<>(additionalTags)
                  : name;
          counters.merge(key, n, Long::sum);
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
