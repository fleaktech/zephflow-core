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

import static io.fleak.zephflow.lib.utils.MiscUtils.METADATA_FS_SOURCE_URN;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_SIZE_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_FILE_SKIPPED_COUNT;
import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
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
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** FsSource with the {@code decoding} field: the formats and compressions it adds. */
class FsSourceDecodingConfigTest {

  private static final String RENAME =
      "then upload the corrected file under a new name; a name already read is not read again";
  private static final Schema SMALL =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"S\",\"fields\":[{\"name\":\"v\",\"type\":\"string\"}]}");

  /** A DLQ entry as a comparable value; the stack trace between its lines varies. */
  record Entry(
      String value,
      Map<String, String> metadata,
      String firstLine,
      String action,
      String lastLine) {}

  @TempDir Path root;
  private final List<Entry> dlq = new ArrayList<>();
  private final List<RecordFleakData> records = new ArrayList<>();
  private final Map<String, Long> counters = new TreeMap<>();
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
  void parquetAvroAndMultilineTextAreDecoded() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message m { required int64 n; }");
    Path parquet = root.resolve("a.parquet");
    SimpleGroupFactory rows = new SimpleGroupFactory(schema);
    writeParquet(
        parquet, schema, List.of(rows.newGroup().append("n", 1L), rows.newGroup().append("n", 2L)));
    run("{\"format\":{\"type\":\"parquet\"}}", Map.of());
    Files.delete(parquet);

    file("a.avro", ocf("null", List.of(small("a"), small("b"))));
    command = null;
    run("{\"format\":{\"type\":\"avro\",\"container\":\"ocf\"}}", Map.of());
    Files.delete(root.resolve("a.avro"));

    file("a.log", "start 1\n  at a\nstart 2\n".getBytes(StandardCharsets.UTF_8));
    command = null;
    run(
        "{\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"startPattern\":\"^start\"}}}",
        Map.of());

    assertEquals(
        List.of(
            json("{\"n\":1}"),
            json("{\"n\":2}"),
            json("{\"v\":\"a\"}"),
            json("{\"v\":\"b\"}"),
            json("{\"__raw__\":\"start 1\\n  at a\"}"),
            json("{\"__raw__\":\"start 2\"}")),
        records);
    assertEquals(List.of(), dlq);
  }

  static Stream<Arguments> badDocuments() {
    FileDescriptorSet set =
        FileDescriptorSet.newBuilder()
            .addFile(
                FileDescriptorProto.newBuilder()
                    .setName("t.proto")
                    .setPackage("t")
                    .addMessageType(
                        DescriptorProto.newBuilder()
                            .setName("M")
                            .addField(
                                FieldDescriptorProto.newBuilder()
                                    .setName("s")
                                    .setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING))))
            .build();
    return Stream.of(
        Arguments.of(
            "yaml",
            "{\"format\":{\"type\":\"yaml\",\"mode\":\"document\"}}",
            "a: [1, 2\n".getBytes(StandardCharsets.UTF_8)),
        Arguments.of(
            "protobuf",
            "{\"format\":{\"type\":\"protobuf\",\"descriptorSet\":\""
                + Base64.getEncoder().encodeToString(set.toByteArray())
                + "\",\"messageType\":\"t.M\",\"framing\":\"single\"}}",
            new byte[] {0x0A, 0x05, 'a'}),
        Arguments.of(
            "avro",
            "{\"format\":{\"type\":\"avro\",\"container\":\"datum\",\"schema\":" + SMALL + "}}",
            new byte[] {0x0A, 'a'}));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("badDocuments")
  void aSmallBadDocumentIsARecordEntryWithItsBytes(String format, String decoding, byte[] bytes)
      throws Exception {
    file("bad.bin", bytes);

    run(decoding, Map.of());

    assertEquals(
        List.of(
            List.of(
                new String(bytes, StandardCharsets.ISO_8859_1),
                Map.of(METADATA_FS_SOURCE_URN, urn("bad.bin")),
                "MALFORMED " + format + " record:",
                "file " + urn("bad.bin") + ", bytes 0-" + bytes.length)),
        dlq.stream()
            .map(
                e ->
                    List.of(
                        e.value(),
                        e.metadata(),
                        e.firstLine().substring(0, e.firstLine().indexOf(':') + 1),
                        e.lastLine()))
            .toList());
  }

  @Test
  void aDamagedAvroBlockIsSummarizedAndTheNextBlocksAreRead() throws Exception {
    byte[] avro =
        ocf("deflate", List.of(small("a")), List.of(small("b".repeat(1_000))), List.of(small("c")));
    byte[] firstBlock = ocf("deflate", List.of(small("a")));
    // Block 2 starts right after block 1's sync: a 1-byte count, a 1-byte size, then deflate data,
    // whose first byte 0xFF declares the invalid block type 3.
    avro[firstBlock.length + 2] = (byte) 0xFF;
    file("a.avro", avro);

    run("{\"format\":{\"type\":\"avro\",\"container\":\"ocf\"}}", Map.of());

    assertEquals(List.of(json("{\"v\":\"a\"}"), json("{\"v\":\"c\"}")), records);
    assertEquals(
        List.of(
            new Entry(
                "",
                Map.of(METADATA_FS_SOURCE_URN, urn("a.avro")),
                "CORRUPT_COMPRESSION avro file: DataFormatException",
                "Action: a block's compressed data is damaged; rewrite the avro file; " + RENAME,
                "file " + urn("a.avro") + "; 1 outcome; recordIndex 2")),
        dlq);
  }

  @Test
  void aParquetRowGroupOverMaxFileBytesAsksForSmallerRowGroups() throws Exception {
    MessageType schema =
        MessageTypeParser.parseMessageType("message m { required binary s (UTF8); }");
    SimpleGroupFactory rows = new SimpleGroupFactory(schema);
    List<Group> groups = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      groups.add(rows.newGroup().append("s", "value-" + i + "-" + "x".repeat(50)));
    }
    writeParquet(root.resolve("a.parquet"), schema, groups);

    run("{\"format\":{\"type\":\"parquet\"}}", Map.of("maxFileBytes", 2000L));

    assertEquals(
        List.of(
            "Action: write the file with smaller row groups (fewer columns if the footer is too"
                + " large), or raise maxFileBytes (and the task's heap to match); "
                + RENAME),
        dlq.stream().map(Entry::action).toList());
  }

  static Stream<Arguments> cutShortCodecs() {
    return Stream.of(
        Arguments.of("bzip2", (Compressor) BZip2CompressorOutputStream::new),
        Arguments.of("snappy", (Compressor) FramedSnappyCompressorOutputStream::new));
  }

  @FunctionalInterface
  interface Compressor {
    OutputStream wrap(OutputStream out) throws IOException;
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cutShortCodecs")
  void aCutShortFileIsAReadFailureAndReadAgain(String codec, Compressor compressor)
      throws Exception {
    StringBuilder lines = new StringBuilder();
    for (int i = 1; i <= 50_000; i++) {
      lines.append("{\"i\":").append(i).append("}\n");
    }
    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (OutputStream out = compressor.wrap(compressed)) {
      out.write(lines.toString().getBytes(StandardCharsets.UTF_8));
    }
    byte[] all = compressed.toByteArray();
    file("a.jsonl." + codec, Arrays.copyOf(all, all.length / 2));
    String decoding =
        "{\"compression\":\"" + codec + "\",\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}";

    for (int run = 0; run < 2; run++) {
      records.clear();
      counters.clear();
      assertThrows(IllegalStateException.class, () -> run(decoding, Map.of()));
      // Only whole batches go out: the open batch is dropped at the cut.
      int emitted = records.size();
      assertEquals(0, emitted % 1000, "emitted: " + emitted);
      Map<String, Long> expected = new TreeMap<>();
      expected.put(METRIC_NAME_INPUT_FILE_SKIPPED_COUNT + "{skip_reason=read_error}", 1L);
      if (emitted > 0) {
        expected.put(METRIC_NAME_INPUT_EVENT_COUNT, (long) emitted);
        expected.put(METRIC_NAME_INPUT_EVENT_SIZE_COUNT, linesLength(emitted) - 1);
      }
      assertEquals(expected, counters);
    }
    assertEquals(List.of(), dlq);
  }

  private void run(String decodingJson, Map<String, Object> extra) throws Exception {
    if (command == null) {
      Map<String, Object> config = new HashMap<>(extra);
      config.put("backend", "file");
      config.put("root", root.toUri().toString());
      config.put("decoding", JsonUtils.fromJsonString(decodingJson, Map.class));
      command = new FsSourceCommand("source", JobContext.builder().build());
      command.parseAndValidateArg(config);
      command.initialize(new RecordingMetricClientProvider());
      ((FsSourceExecutionContext) command.getExecutionContext()).dlqWriter =
          new CapturingDlqWriter();
    }
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
  }

  private static void writeParquet(Path file, MessageType schema, List<Group> groups)
      throws IOException {
    try (OutputStream out = Files.newOutputStream(file);
        ParquetWriter<Group> writer =
            ExampleParquetWriter.builder(new StreamOutputFile(out))
                .withType(schema)
                .withConf(new Configuration(false))
                .build()) {
      for (Group group : groups) {
        writer.write(group);
      }
    }
  }

  /** A parquet output over a stream. */
  private record StreamOutputFile(OutputStream out) implements OutputFile {
    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return new PositionOutputStream() {
        private long pos;

        @Override
        public long getPos() {
          return pos;
        }

        @Override
        public void write(int b) throws IOException {
          out.write(b);
          pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          out.write(b, off, len);
          pos += len;
        }
      };
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }
  }

  @SafeVarargs
  private static byte[] ocf(String codec, List<GenericRecord>... blocks) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataFileWriter<GenericRecord> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(SMALL))) {
      writer.setCodec(CodecFactory.fromString(codec));
      writer.create(SMALL, bytes);
      for (List<GenericRecord> block : blocks) {
        for (GenericRecord record : block) {
          writer.append(record);
        }
        writer.sync();
      }
    }
    return bytes.toByteArray();
  }

  private static GenericRecord small(String value) {
    GenericRecord record = new GenericData.Record(SMALL);
    record.put("v", value);
    return record;
  }

  /** The length of the lines {"i":1} to {"i":count}. */
  private static long linesLength(int count) {
    long length = 0;
    for (int i = 1; i <= count; i++) {
      length += ("{\"i\":" + i + "}\n").length();
    }
    return length;
  }

  private static RecordFleakData json(String json) {
    return (RecordFleakData) JsonUtils.loadFleakDataFromJsonString(json);
  }

  private void file(String name, byte[] content) throws IOException {
    Files.write(root.resolve(name), content);
  }

  private String urn(String name) {
    return root.resolve(name).toUri().toString();
  }

  private class CapturingDlqWriter extends DlqWriter {
    @Override
    protected void doWrite(DeadLetter deadLetter) {
      List<String> lines = deadLetter.getErrorMessage().lines().toList();
      Map<String, String> metadata = new HashMap<>();
      deadLetter.getMetadata().forEach((k, v) -> metadata.put(k.toString(), v.toString()));
      dlq.add(
          new Entry(
              StandardCharsets.ISO_8859_1.decode(deadLetter.getValue().duplicate()).toString(),
              metadata,
              lines.getFirst(),
              lines.get(1),
              lines.getLast()));
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
          if (n == 0) {
            return;
          }
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
