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

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_DESER_ERR_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_SIZE_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_FILE_SKIPPED_COUNT;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * What FsSource emits, checkpoints and dead-letters for a fixed set of files, against expected
 * results under {@code fssource/snapshot/}. A change of read path must change only the expected
 * results of the cases it means to change. {@code -Dfssource.snapshot.update=true} rewrites them
 * from the current code and fails, so that the diff is reviewed.
 */
class FsSourceSnapshotTest {

  private static final Path EXPECTED = Path.of("src/test/resources/fssource/snapshot");
  private static final boolean UPDATE = Boolean.getBoolean("fssource.snapshot.update");
  private static final Set<String> RECORDED_COUNTERS =
      Set.of(
          METRIC_NAME_INPUT_EVENT_COUNT,
          METRIC_NAME_INPUT_EVENT_SIZE_COUNT,
          METRIC_NAME_INPUT_DESER_ERR_COUNT,
          METRIC_NAME_INPUT_FILE_SKIPPED_COUNT);
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(SerializationFeature.INDENT_OUTPUT)
          .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

  /** One file read by one source configuration. */
  record Case(
      String name, String fileName, byte[] content, Map<String, Object> config, boolean dlq) {
    @Override
    public String toString() {
      return name;
    }
  }

  @TempDir Path root;
  private final List<DeadLetter> deadLetters = new ArrayList<>();
  private final Map<String, Long> counters = new TreeMap<>();

  @BeforeEach
  void registerBackend() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
  }

  static Stream<Arguments> cases() {
    String lines = "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n";
    String csv = "id,name\n1,x\n2,y\n";
    String manyLines =
        IntStream.rangeClosed(1, 3000)
            .mapToObj(
                i -> "{\"line\":" + i + ",\"pad\":\"" + Integer.toHexString(i * 7919) + "\"}\n")
            .collect(Collectors.joining());
    StringBuilder chunked = new StringBuilder();
    IntStream.rangeClosed(1, 10)
        .forEach(i -> chunked.append(i == 8 ? "{bad\n" : "{\"line\":" + i + "}\n"));
    String wideHeader =
        IntStream.range(0, 16_385).mapToObj(i -> "c" + i).collect(Collectors.joining(","))
            + "\n"
            + IntStream.range(0, 16_385).mapToObj(i -> "1").collect(Collectors.joining(","))
            + "\n";
    return Stream.of(
            jsonl("json_object_line_valid", lines),
            jsonl("json_object_line_bad_record", "{\"a\":1}\n{\"a\":\n{\"a\":3}\n"),
            new Case(
                "json_object_line_gzip",
                "data.jsonl.gz",
                gzip(lines),
                legacy("JSON_OBJECT_LINE"),
                true),
            new Case(
                "json_object_line_gzip_trailer_cut",
                "data.jsonl.gz",
                Arrays.copyOf(gzip(lines), gzip(lines).length - 6),
                legacy("JSON_OBJECT_LINE"),
                true),
            new Case(
                "json_object_line_gzip_cut_mid_data",
                "data.jsonl.gz",
                Arrays.copyOf(gzip(manyLines), gzip(manyLines).length / 2),
                legacy("JSON_OBJECT_LINE"),
                true),
            new Case(
                "json_object_line_gzip_trailing_bytes",
                "data.jsonl.gz",
                concat(gzip(lines), "trailing".getBytes(StandardCharsets.UTF_8)),
                legacy("JSON_OBJECT_LINE"),
                true),
            jsonl("json_object_line_empty", ""),
            new Case("json_object_empty", "data.json", new byte[0], legacy("JSON_OBJECT"), true),
            new Case(
                "json_object_empty_no_dlq", "data.json", new byte[0], legacy("JSON_OBJECT"), false),
            new Case("json_array_empty", "data.json", new byte[0], legacy("JSON_ARRAY"), true),
            new Case("xml_empty", "data.xml", new byte[0], legacy("XML"), true),
            new Case("text_empty", "data.txt", new byte[0], legacy("TEXT"), true),
            new Case("string_line_empty", "data.txt", new byte[0], legacy("STRING_LINE"), true),
            csv("csv_empty", ""),
            new Case(
                "json_object_line_long_line_within_a_read",
                "data.jsonl",
                bytes("{\"a\":1}\n{\"long\":\"" + "x".repeat(100) + "\"}\n{\"a\":3}\n"),
                with(legacy("JSON_OBJECT_LINE"), "maxFileBytes", 64L),
                true),
            new Case(
                "json_object_line_cr_only_over_limit",
                "data.jsonl",
                bytes("{\"a\":1}\r{\"a\":2}\r{\"a\":3}\r"),
                with(with(legacy("JSON_OBJECT_LINE"), "maxFileBytes", 16L), "chunkSizeBytes", 8),
                true),
            new Case(
                "csv_record_over_limit_within_slack",
                "data.csv",
                bytes("id,name\n1,x\n2," + "y".repeat(200) + "\n3,z\n"),
                with(with(legacy("CSV"), "maxFileBytes", 64L), "chunkSizeBytes", 16),
                true),
            new Case(
                "csv_record_over_limit_past_slack",
                "data.csv",
                bytes("id,name\n1,x\n2," + "y".repeat(200_000) + "\n3,z\n"),
                with(legacy("CSV"), "maxFileBytes", 1000L),
                true),
            jsonl("json_object_line_array_line", "{\"a\":1}\n[{\"a\":2}]\n{\"a\":3}\n"),
            new Case(
                "json_object_line_oversized_line",
                "data.jsonl",
                bytes("{\"a\":1}\n{\"long\":\"" + "x".repeat(20_000) + "\"}\n{\"a\":3}\n"),
                with(
                    with(legacy("JSON_OBJECT_LINE"), "maxFileBytes", 10_000L),
                    "chunkSizeBytes",
                    16),
                true),
            new Case(
                "json_object_line_index_past_chunks",
                "data.jsonl",
                bytes(chunked.toString()),
                with(legacy("JSON_OBJECT_LINE"), "chunkSizeBytes", 32),
                true),
            new Case(
                "json_object_line_all_bad_no_dlq",
                "data.jsonl",
                bytes("{bad\nnot json\n"),
                legacy("JSON_OBJECT_LINE"),
                false),
            new Case(
                "json_object_valid",
                "data.json",
                bytes("{\"a\":{\"b\":[1,2]}}"),
                legacy("JSON_OBJECT"),
                true),
            new Case("json_object_bad", "data.json", bytes("{\"a\":"), legacy("JSON_OBJECT"), true),
            new Case(
                "json_object_over_limit",
                "data.json",
                bytes("{\"long\":\"" + "x".repeat(100) + "\"}"),
                with(legacy("JSON_OBJECT"), "maxFileBytes", 32L),
                true),
            new Case(
                "json_array_valid",
                "data.json",
                bytes("[{\"a\":1},{\"a\":2}]"),
                legacy("JSON_ARRAY"),
                true),
            new Case(
                "json_array_bad",
                "data.json",
                bytes("[{\"a\":1},{\"a\":,{\"a\":3}]"),
                legacy("JSON_ARRAY"),
                true),
            new Case(
                "string_line_valid",
                "data.txt",
                bytes("one\ntwo\r\nthree\n"),
                legacy("STRING_LINE"),
                true),
            new Case(
                "string_line_cr_only_over_limit",
                "data.txt",
                bytes("first line\rsecond line\rthird line\r"),
                with(with(legacy("STRING_LINE"), "maxFileBytes", 16L), "chunkSizeBytes", 8),
                true),
            new Case("text_valid", "data.txt", bytes("one\ntwo\n"), legacy("TEXT"), true),
            new Case(
                "xml_valid",
                "data.xml",
                bytes("<root><a>1</a><b>x</b></root>"),
                legacy("XML"),
                true),
            new Case("xml_bad", "data.xml", bytes("<root><a>1</a>"), legacy("XML"), true),
            csv("csv_valid", csv),
            csv("csv_duplicate_header", "id,id\n1,2\n"),
            csv("csv_empty_header_name", "id,\n1,2\n"),
            csv("csv_quote_left_open", "id,name\n1,\"open\n2,y\n"),
            csv("csv_text_after_closing_quote", "id,name\n1,\"x\"y\n2,z\n"),
            csv("csv_short_row", "id,name\n\"1\"\n2,y\n"),
            csv("csv_header_over_16384_columns", wideHeader))
        .map(Arguments::of);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void matchesTheSnapshot(Case c) throws Exception {
    Files.write(root.resolve(c.fileName()), c.content());
    FsSourceCommand command = new FsSourceCommand("source", JobContext.builder().build());
    Map<String, Object> config = new HashMap<>(c.config());
    config.put("backend", "file");
    config.put("root", root.toUri().toString());
    command.parseAndValidateArg(config);
    command.initialize(new RecordingMetricClientProvider());
    if (c.dlq()) {
      ((FsSourceExecutionContext) command.getExecutionContext()).dlqWriter =
          new CapturingDlqWriter();
    }

    Map<String, Object> first = run(command);
    Map<String, Object> second = run(command);
    Map<String, Object> actual = new LinkedHashMap<>(first);
    actual.put("secondRunReadTheFileAgain", !second.equals(emptyRun()));
    JsonNode actualTree = MAPPER.readTree(MAPPER.writeValueAsString(actual));

    Path expectedFile = EXPECTED.resolve(c.name() + ".json");
    if (UPDATE) {
      Files.createDirectories(EXPECTED);
      MAPPER.writeValue(expectedFile.toFile(), actualTree);
      fail("updated " + expectedFile + "; review the diff and run again without the update flag");
    }
    assertEquals(MAPPER.readTree(expectedFile.toFile()), actualTree);
  }

  /** One run's records, DLQ entries (value and metadata), outcome and counters. */
  private Map<String, Object> run(FsSourceCommand command) {
    deadLetters.clear();
    counters.clear();
    List<RecordFleakData> records = new ArrayList<>();
    String failure = null;
    try {
      command.execute(
          "user",
          new SourceEventAcceptor() {
            @Override
            public void accept(List<RecordFleakData> batch) {
              records.addAll(batch);
            }

            @Override
            public void terminate() {}
          });
    } catch (Exception e) {
      failure = e.getClass().getSimpleName();
    }
    Map<String, Object> run = new LinkedHashMap<>();
    run.put("records", records.stream().map(FsSourceSnapshotTest::recorded).toList());
    run.put(
        "dlq",
        deadLetters.stream()
            .map(
                d -> {
                  Map<String, Object> entry = new LinkedHashMap<>();
                  entry.put(
                      "value",
                      d.getValue() == null
                          ? null
                          : StandardCharsets.UTF_8.decode(d.getValue().duplicate()).toString());
                  entry.put(
                      "metadata",
                      d.getMetadata() == null
                          ? Map.of()
                          : d.getMetadata().entrySet().stream()
                              .collect(
                                  Collectors.toMap(
                                      e -> e.getKey().toString(),
                                      e -> relative(e.getValue().toString()))));
                  return entry;
                })
            .toList());
    run.put("failed", failure);
    run.put("counters", new TreeMap<>(counters));
    return run;
  }

  /** The record, or for one of more than 100 fields only its field count. */
  private static Map<String, Object> recorded(RecordFleakData record) {
    Map<String, Object> fields = record.unwrap();
    return fields.size() > 100 ? Map.of("<fields>", fields.size()) : fields;
  }

  private Map<String, Object> emptyRun() {
    Map<String, Object> run = new LinkedHashMap<>();
    run.put("records", List.of());
    run.put("dlq", List.of());
    run.put("failed", null);
    run.put("counters", Map.of());
    return run;
  }

  private String relative(String value) {
    return value.replace(root.toUri().toString(), "<root>/");
  }

  private static Case jsonl(String name, String content) {
    return new Case(name, "data.jsonl", bytes(content), legacy("JSON_OBJECT_LINE"), true);
  }

  private static Case csv(String name, String content) {
    return new Case(name, "data.csv", bytes(content), legacy("CSV"), true);
  }

  private static Map<String, Object> legacy(String encodingType) {
    return Map.of("encodingType", encodingType);
  }

  private static Map<String, Object> with(Map<String, Object> config, String key, Object value) {
    Map<String, Object> out = new HashMap<>(config);
    out.put(key, value);
    return out;
  }

  private static byte[] bytes(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] gzip(String s) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
      gz.write(bytes(s));
    } catch (IOException e) {
      throw new AssertionError(e);
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
      deadLetters.add(deadLetter);
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
          if (!RECORDED_COUNTERS.contains(name)) {
            return;
          }
          String key =
              additionalTags == null || additionalTags.isEmpty()
                  ? name
                  : name + new TreeMap<>(additionalTags);
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
