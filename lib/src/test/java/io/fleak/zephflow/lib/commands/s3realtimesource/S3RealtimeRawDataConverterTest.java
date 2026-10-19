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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.github.luben.zstd.Zstd;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.NodeExecutionException;
import io.fleak.zephflow.lib.commands.source.BatchEmittingConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.RecordCursor;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3RealtimeRawDataConverterTest {

  private static final long MIB = 1024L * 1024;
  private static final long HEAP = 2048 * MIB;
  private static final long MAX_OBJECT_SIZE = 64 * MIB;
  private static final String NOTIFICATION = "{\"Records\":[]}";

  /**
   * A DLQ entry as a comparable value: the value as text, the first line, the action line (null
   * when the second line is not one), the last line of the error text (the stack trace between them
   * varies), and the node id.
   */
  record Entry(String value, String firstLine, String action, String lastLine, String nodeId) {}

  private static final String RETRY = "then re-upload the object or send this notification again";
  private static final String RECORD_ACTION =
      "Action: fix this record where it is written, or the source's encodingType or decoding if"
          + " every record fails like this; the value is the record's bytes";
  private static final String READ_ACTION =
      "Action: check the cause below, such as the source's S3 permissions (s3:GetObject), the"
          + " network, or free disk where the task runs; "
          + RETRY;
  private static final String CORRUPT_ACTION =
      "Action: check the source's compression setting against the object, and that the object was"
          + " complete when it was announced; otherwise its compressed data (for avro, possibly a"
          + " block's own codec data) is damaged or has bytes after its end, so rewrite the object; "
          + RETRY;
  private static final String TOO_LARGE_ACTION =
      "Action: raise maxObjectSizeBytes above the object's size, with that much free disk for the"
          + " task and a visibilityTimeoutSeconds long enough to download it (lower"
          + " maxNumberOfMessages if several arrive together); "
          + RETRY;
  private static final String LINE_FRAMING =
      ", or write smaller records; if records should be much smaller, check that the source's"
          + " format matches the object (a wrong format or framing, or an unclosed csv quote, joins"
          + " many records into one); ";
  private static final String RECORD_LIMIT_ACTION =
      "Action: this source sets maxRecordBytes to 1/128 of the task's heap, so give the task a"
          + " heap of at least 128 times the record's size"
          + LINE_FRAMING
          + RETRY;

  private static String malformedAction(String format) {
    return "Action: check that the object is "
        + format
        + " as the source's encodingType or decoding says, or fix the listed records where the"
        + " object is written; "
        + RETRY;
  }

  @TempDir Path spoolDir;
  private S3Client s3Client;
  private Queue<String> confirmed;
  private DlqWriter dlqWriter;
  private final List<Entry> dlq = new ArrayList<>();
  private final List<List<String>> texts = new ArrayList<>();
  private FleakCounter skippedObjectCounter;
  private FleakCounter dataSizeCounter;
  private FleakCounter inputEventCounter;
  private FleakCounter deserializeFailureCounter;
  private SourceExecutionContext<S3EventMessage> context;

  /** Records the batches; can make one batch fail downstream or stop the source after one. */
  private static final class TestEmitter implements BatchEmittingConverter.Emitter {
    final List<List<RecordFleakData>> batches = new ArrayList<>();
    int failBatch = -1;
    boolean failEvenBatches;
    int stopAfterBatch = -1;
    boolean stopped;

    @Override
    public void accept(List<RecordFleakData> batch) {
      batches.add(List.copyOf(batch));
      if (batches.size() - 1 == stopAfterBatch) {
        stopped = true;
      }
      if (batches.size() - 1 == failBatch || (failEvenBatches && batches.size() % 2 == 0)) {
        if (stopWhenFailing) {
          stopped = true;
        }
        throw new NodeExecutionException("eval_1", "eval", "missing field", null);
      }
    }

    int stopAtCall = -1;
    boolean stopWhenFailing;
    private int calls;

    @Override
    public boolean stopped() {
      if (++calls == stopAtCall) {
        stopped = true;
      }
      return stopped;
    }

    List<RecordFleakData> records() {
      return batches.stream().flatMap(List::stream).toList();
    }
  }

  @BeforeEach
  void setUp() {
    s3Client = mock(S3Client.class);
    confirmed = new ConcurrentLinkedQueue<>();
    dlqWriter = mock(DlqWriter.class);
    doAnswer(
            invocation -> {
              SerializedEvent value = invocation.getArgument(1);
              String text = invocation.getArgument(2);
              List<String> lines = text.lines().toList();
              texts.add(lines);
              dlq.add(
                  new Entry(
                      new String(value.value(), StandardCharsets.UTF_8),
                      lines.getFirst(),
                      lines.size() > 1 && lines.get(1).startsWith("Action: ") ? lines.get(1) : null,
                      lines.getLast(),
                      invocation.getArgument(3)));
              return null;
            })
        .when(dlqWriter)
        .writeToDlq(anyLong(), any(), anyString(), anyString());
    skippedObjectCounter = mock(FleakCounter.class);
    dataSizeCounter = mock(FleakCounter.class);
    inputEventCounter = mock(FleakCounter.class);
    deserializeFailureCounter = mock(FleakCounter.class);
    context =
        new SourceExecutionContext<>(
            null, null, null, dataSizeCounter, inputEventCounter, deserializeFailureCounter, null);
  }

  private S3RealtimeRawDataConverter converter(S3Decoding decoding, long heap, long batchBytes) {
    return new S3RealtimeRawDataConverter(
        new S3RegionalClientProvider(region -> s3Client),
        new S3ObjectDecoder(decoding, heap),
        decoding.formatName(),
        MAX_OBJECT_SIZE,
        spoolDir,
        batchBytes,
        confirmed,
        dlqWriter,
        new S3RealtimeRawDataEncoder(),
        "node",
        skippedObjectCounter);
  }

  private S3RealtimeRawDataConverter converter(EncodingType encodingType) {
    return converter(new S3Decoding(encodingType, null, null, false), HEAP, 4 * MIB);
  }

  private static S3EventMessage message(String... keys) {
    return new S3EventMessage(
        "m1",
        "r1",
        NOTIFICATION,
        Arrays.stream(keys).map(k -> new S3ObjectRef("b", k, "us-east-1")).toList());
  }

  private void stub(String key, byte[] data) {
    when(s3Client.getObject(argThat((GetObjectRequest r) -> r != null && key.equals(r.key()))))
        .thenAnswer(
            invocation ->
                new ResponseInputStream<>(
                    GetObjectResponse.builder()
                        .contentLength((long) data.length)
                        .versionId("v1")
                        .build(),
                    AbortableInputStream.create(new ByteArrayInputStream(data))));
  }

  private TestEmitter emit(S3RealtimeRawDataConverter converter, S3EventMessage message) {
    TestEmitter emitter = new TestEmitter();
    converter.emit(message, context, emitter);
    return emitter;
  }

  @Test
  void jsonLinesAreEmittedAndTheHandleIsQueuedOnlyOnAcknowledge() {
    stub("k.jsonl", utf8("{\"msg\":\"a\"}\n{\"msg\":\"b\"}"));
    S3RealtimeRawDataConverter converter = converter(EncodingType.JSON_OBJECT_LINE);
    S3EventMessage message = message("k.jsonl");

    TestEmitter emitter = emit(converter, message);

    assertEquals(
        List.of(List.of(json("{\"msg\":\"a\"}"), json("{\"msg\":\"b\"}"))), emitter.batches);
    assertEquals(List.of(), List.copyOf(confirmed));
    converter.acknowledge(message);
    assertEquals(List.of("r1"), List.copyOf(confirmed));
    assertEquals(List.of(), dlq);
    verify(inputEventCounter).increase(eq(2L), anyMap());
    verify(dataSizeCounter).increase(eq(23L), anyMap());
  }

  @Test
  void addS3MetadataEnrichesRecords() {
    stub("dir/k.json", utf8("{\"msg\":\"a\"}"));
    S3RealtimeRawDataConverter converter =
        converter(new S3Decoding(EncodingType.JSON_OBJECT_LINE, null, null, true), HEAP, 4 * MIB);

    TestEmitter emitter = emit(converter, message("dir/k.json"));

    Map<String, Object> expected = new LinkedHashMap<>();
    expected.put("msg", "a");
    expected.put("__s3_bucket", "b");
    expected.put("__s3_key", "dir/k.json");
    assertEquals(List.of(record(expected)), emitter.records());
  }

  @Test
  void objectsAreDecodedOneAtATimeInOrder() {
    stub("k1.jsonl", utf8("{\"msg\":\"a\"}"));
    stub("k2.jsonl", utf8("{\"msg\":\"b\"}"));

    TestEmitter emitter =
        emit(converter(EncodingType.JSON_OBJECT_LINE), message("k1.jsonl", "k2.jsonl"));

    assertEquals(
        List.of(List.of(json("{\"msg\":\"a\"}")), List.of(json("{\"msg\":\"b\"}"))),
        emitter.batches);
    verify(s3Client, times(2)).getObject(any(GetObjectRequest.class));
  }

  @Test
  void gzipIsDetectedAndForced() {
    stub("data.jsonl.gz", gzip(utf8("{\"msg\":\"a\"}\n{\"msg\":\"b\"}")));
    List<RecordFleakData> expected = List.of(json("{\"msg\":\"a\"}"), json("{\"msg\":\"b\"}"));

    assertEquals(
        expected,
        emit(converter(EncodingType.JSON_OBJECT_LINE), message("data.jsonl.gz")).records());
    assertEquals(
        expected,
        emit(
                converter(
                    new S3Decoding(
                        EncodingType.JSON_OBJECT_LINE, CompressionType.GZIP, null, false),
                    HEAP,
                    4 * MIB),
                message("data.jsonl.gz"))
            .records());
  }

  @Test
  void theInputSizeIsTheDecompressedEndOfTheLastSpan() {
    stub("data.jsonl.gz", gzip(utf8("{\"msg\":\"a\"}\n{\"msg\":\"b\"}\n")));

    emit(converter(EncodingType.JSON_OBJECT_LINE), message("data.jsonl.gz"));

    verify(dataSizeCounter).increase(eq(23L), anyMap());
  }

  @Test
  void csvParquetMultilineAndZstdDecode() throws IOException {
    stub("x.csv", utf8("a,b\n1,2\n"));
    assertEquals(
        List.of(json("{\"a\":\"1\",\"b\":\"2\"}")),
        emit(converter(EncodingType.CSV), message("x.csv")).records());

    MessageType schema = MessageTypeParser.parseMessageType("message m { required int64 n; }");
    ByteArrayOutputStream parquet = new ByteArrayOutputStream();
    SimpleGroupFactory rows = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(new MemoryOutputFile(parquet))
            .withType(schema)
            .withConf(new Configuration(false))
            .build()) {
      writer.write(rows.newGroup().append("n", 1L));
      writer.write(rows.newGroup().append("n", 2L));
    }
    stub("x.parquet", parquet.toByteArray());
    assertEquals(
        List.of(json("{\"n\":1}"), json("{\"n\":2}")),
        emit(
                converter(decoding("{\"format\":{\"type\":\"parquet\"}}"), HEAP, 4 * MIB),
                message("x.parquet"))
            .records());

    stub("x.log", utf8("start 1\n  at a\nstart 2\n"));
    assertEquals(
        List.of(json("{\"__raw__\":\"start 1\\n  at a\"}"), json("{\"__raw__\":\"start 2\"}")),
        emit(
                converter(
                    decoding(
                        "{\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"startPattern\":\"^start\"}}}"),
                    HEAP,
                    4 * MIB),
                message("x.log"))
            .records());

    stub("x.zst", Zstd.compress(utf8("{\"a\":1}\n")));
    assertEquals(
        List.of(json("{\"a\":1}")),
        emit(
                converter(
                    decoding(
                        "{\"compression\":\"zstd\",\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}"),
                    HEAP,
                    4 * MIB),
                message("x.zst"))
            .records());
  }

  @Test
  void batchesCloseAtAThousandRecordsOrTheSizeCap() {
    StringBuilder lines = new StringBuilder();
    for (int i = 0; i < 2500; i++) {
      lines.append("{\"i\":").append(i).append("}\n");
    }
    stub("many.jsonl", utf8(lines.toString()));
    assertEquals(
        List.of(1000, 1000, 500),
        emit(converter(EncodingType.JSON_OBJECT_LINE), message("many.jsonl")).batches.stream()
            .map(List::size)
            .toList());

    stub("big.txt", utf8("x".repeat(100) + "\n" + "y".repeat(100) + "\n" + "z".repeat(100) + "\n"));
    // each record is estimated at 8 + 8 + 7 + 8 + 100 = 131 bytes; a 200-byte cap closes every
    // second record
    assertEquals(
        List.of(2, 1),
        emit(
                converter(new S3Decoding(EncodingType.STRING_LINE, null, null, false), HEAP, 200),
                message("big.txt"))
            .batches
            .stream()
            .map(List::size)
            .toList());
  }

  @Test
  void aBadRecordIsDeadLetteredWithItsBytesAndTheRestIsEmitted() {
    stub("bad.jsonl", utf8("{\"a\":1}\nnot json\n{\"a\":3}\n"));

    TestEmitter emitter = emit(converter(EncodingType.JSON_OBJECT_LINE), message("bad.jsonl"));

    assertEquals(List.of(json("{\"a\":1}"), json("{\"a\":3}")), emitter.records());
    assertEquals(
        List.of(
            new Entry(
                "not json",
                "MALFORMED json record: JsonParseException",
                RECORD_ACTION,
                "object s3://b/bad.jsonl, recordIndex 2, bytes 8-16",
                "node")),
        dlq);
    verify(deserializeFailureCounter).increase(Map.of());
  }

  @Test
  void twoDifferentSyntaxErrorsShareTheFirstLine() {
    stub("bad.jsonl", utf8("{\"a\" 1}\n{\"a\":}\n"));

    emit(converter(EncodingType.JSON_OBJECT_LINE), message("bad.jsonl"));

    assertEquals(
        List.of(
            "MALFORMED json record: JsonParseException",
            "MALFORMED json record: JsonParseException"),
        dlq.stream().map(Entry::firstLine).toList());
  }

  @Test
  void aBadRecordOver128KibIsSummarized() {
    String big = "x".repeat(S3RealtimeRawDataConverter.MAX_RECORD_ENTRY_BYTES + 1);
    stub("bad.jsonl", utf8("{\"a\":1}\n" + big + "\n"));

    emit(converter(EncodingType.JSON_OBJECT_LINE), message("bad.jsonl"));

    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "MALFORMED json object: JsonParseException",
                malformedAction("json"),
                "object s3://b/bad.jsonl version v1; 1 outcome; recordIndex 2 (bytes 8-"
                    + (8 + big.length())
                    + ")",
                "node")),
        dlq);
  }

  @Test
  void oversizedRecordsAreSummarizedWithTheirRanges() {
    String big = "x".repeat(9000);
    stub("big.txt", utf8(big + "\nok\n" + big + "\n" + big + "\n"));

    TestEmitter emitter =
        emit(
            converter(new S3Decoding(EncodingType.STRING_LINE, null, null, false), MIB, 4 * MIB),
            message("big.txt"));

    assertEquals(List.of(json("{\"__raw__\":\"ok\"}")), emitter.records());
    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "OVERSIZED text object: maxRecordBytes",
                RECORD_LIMIT_ACTION,
                "object s3://b/big.txt version v1; 3 outcomes; recordIndex 1 (bytes 0-9000), 3-4 (bytes 9004-27005)",
                "node")),
        dlq);
    assertEquals(
        List.of(
            "OVERSIZED text object: maxRecordBytes",
            RECORD_LIMIT_ACTION,
            "a record reached 9000 bytes, over maxRecordBytes 8192",
            "object s3://b/big.txt version v1; 3 outcomes; recordIndex 1 (bytes 0-9000), 3-4 (bytes 9004-27005)"),
        texts.getFirst());
    verify(deserializeFailureCounter, times(3)).increase(Map.of());
    verify(dataSizeCounter).increase(eq(27005L), anyMap());
  }

  @Test
  void manyBytesLessFailuresGiveOneSummaryListingAtMost100Ranges() {
    StringBuilder text = new StringBuilder();
    String big = "x".repeat(9000);
    for (int i = 0; i < 300; i++) {
      text.append(big).append("\nok\n");
    }
    stub("big.txt", utf8(text.toString()));

    emit(
        converter(new S3Decoding(EncodingType.STRING_LINE, null, null, false), MIB, 4 * MIB),
        message("big.txt"));

    StringBuilder ranges = new StringBuilder("recordIndex ");
    for (int i = 0; i < 100; i++) {
      ranges.append(i == 0 ? "" : ", ").append(2 * i + 1);
      ranges.append(" (bytes ").append(9004L * i).append('-').append(9004L * i + 9000).append(')');
    }
    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "OVERSIZED text object: maxRecordBytes",
                RECORD_LIMIT_ACTION,
                "object s3://b/big.txt version v1; 300 outcomes; "
                    + ranges
                    + ", and 200 more ranges",
                "node")),
        dlq);
  }

  @Test
  void aTerminalFailureEndsTheObjectAfterTheRecordsBeforeIt() {
    stub("x.csv", utf8("a,b\n1,2\n\"3,4\n"));

    TestEmitter emitter = emit(converter(EncodingType.CSV), message("x.csv"));

    assertEquals(List.of(json("{\"a\":\"1\",\"b\":\"2\"}")), emitter.records());
    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "MALFORMED csv object: MalformedCsvException",
                malformedAction("csv"),
                "object s3://b/x.csv version v1; 1 outcome; recordIndex 2; the object ended at recordIndex 2",
                "node")),
        dlq);
  }

  @Test
  void anObjectOverMaxObjectSizeIsDeadLetteredAndSkipped() {
    when(s3Client.getObject(any(GetObjectRequest.class)))
        .thenReturn(
            new ResponseInputStream<>(
                GetObjectResponse.builder()
                    .contentLength(MAX_OBJECT_SIZE + 1)
                    .versionId("v2")
                    .build(),
                AbortableInputStream.create(new ByteArrayInputStream(new byte[0]))));

    TestEmitter emitter = emit(converter(EncodingType.JSON_OBJECT_LINE), message("big.json"));

    assertEquals(List.of(), emitter.batches);
    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "OBJECT_TOO_LARGE s3 object",
                TOO_LARGE_ACTION,
                "object s3://b/big.json version v2 size "
                    + (MAX_OBJECT_SIZE + 1)
                    + " exceeds maxObjectSizeBytes "
                    + MAX_OBJECT_SIZE,
                "node")),
        dlq);
    verify(skippedObjectCounter).increase(Map.of("reason", "oversized"));
  }

  @Test
  void aMissingObjectIsSkippedWithoutADlqEntry() {
    when(s3Client.getObject(any(GetObjectRequest.class)))
        .thenThrow(NoSuchKeyException.builder().message("gone").build());

    TestEmitter emitter = emit(converter(EncodingType.JSON_OBJECT_LINE), message("gone.jsonl"));

    assertEquals(List.of(), emitter.batches);
    assertEquals(List.of(), dlq);
    verify(skippedObjectCounter).increase(Map.of("reason", "missing"));
  }

  @Test
  void aFailedDownloadIsAReadFailureSummary() {
    when(s3Client.getObject(any(GetObjectRequest.class)))
        .thenThrow(new IllegalStateException("s3 unavailable"));

    emit(converter(EncodingType.JSON_OBJECT_LINE), message("k.json"));

    assertEquals(1, dlq.size());
    assertEquals(
        new Entry(
            NOTIFICATION,
            "READ_FAILURE s3 object: IllegalStateException",
            READ_ACTION,
            "object s3://b/k.json; 1 outcome; the object ended",
            "node"),
        dlq.getFirst());
    verify(deserializeFailureCounter).increase(Map.of());
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "invalid-region"})
  void anInvalidRegionIsDeadLetteredWithoutDiscardingTheValidSibling(String region) {
    stub("good.jsonl", utf8("{\"msg\":\"good\"}"));
    S3EventMessage message =
        new S3EventMessage(
            "m1",
            "r1",
            NOTIFICATION,
            List.of(
                new S3ObjectRef("b", "invalid.jsonl", region),
                new S3ObjectRef("b", "good.jsonl", "us-east-1")));

    TestEmitter emitter = emit(converter(EncodingType.JSON_OBJECT_LINE), message);

    assertEquals(List.of(json("{\"msg\":\"good\"}")), emitter.records());
    assertEquals(1, dlq.size());
    assertEquals(
        "object s3://b/invalid.jsonl; 1 outcome; the object ended", dlq.getFirst().lastLine());
    assertTrue(
        dlq.getFirst().firstLine().startsWith("READ_FAILURE s3 object: "),
        dlq.getFirst().firstLine());
    verify(s3Client, never())
        .getObject(argThat((GetObjectRequest r) -> "invalid.jsonl".equals(r.key())));
  }

  @Test
  void aDownstreamFailureIsSummarizedPerNodeAndTheNextBatchGoesOn() {
    StringBuilder lines = new StringBuilder();
    for (int i = 1; i <= 2500; i++) {
      lines.append("{\"i\":").append(i).append("}\n");
    }
    stub("many.jsonl", utf8(lines.toString()));
    TestEmitter emitter = new TestEmitter();
    emitter.failBatch = 1;

    converter(EncodingType.JSON_OBJECT_LINE).emit(message("many.jsonl"), context, emitter);

    assertEquals(List.of(1000, 1000, 500), emitter.batches.stream().map(List::size).toList());
    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "io.fleak.zephflow.lib.commands.NodeExecutionException: missing field",
                null,
                "object s3://b/many.jsonl version v1; 1 batch failed; recordIndex 1001-2000",
                "eval_1")),
        dlq);
  }

  @Test
  void aStopEndsTheObjectWithNoSummaries() {
    StringBuilder lines = new StringBuilder();
    for (int i = 1; i <= 2500; i++) {
      lines.append(i == 2200 ? "not json" : "{\"i\":" + i + "}").append('\n');
    }
    stub("many.jsonl", utf8(lines.toString()));
    stub("next.jsonl", utf8("{\"a\":1}"));
    TestEmitter emitter = new TestEmitter();
    emitter.stopAfterBatch = 0;

    converter(EncodingType.JSON_OBJECT_LINE)
        .emit(message("many.jsonl", "next.jsonl"), context, emitter);

    assertEquals(List.of(1000), emitter.batches.stream().map(List::size).toList());
    assertEquals(List.of(), dlq);
    verify(s3Client, never())
        .getObject(argThat((GetObjectRequest r) -> "next.jsonl".equals(r.key())));
  }

  @Test
  void theCauseNameIsTheInnermostNonSourceReadExceptionWithItsAwsCode() {
    assertEquals(
        List.of("IllegalStateException", "S3Exception AccessDenied", "SourceReadException"),
        List.of(
            S3RealtimeRawDataConverter.causeName(
                new SourceReadException("x", new IllegalStateException("y"))),
            S3RealtimeRawDataConverter.causeName(
                new SourceReadException(
                    "x",
                    S3Exception.builder()
                        .awsErrorDetails(
                            AwsErrorDetails.builder().errorCode("AccessDenied").build())
                        .build())),
            S3RealtimeRawDataConverter.causeName(new SourceReadException("x", null))));
  }

  @Test
  void theCauseNameLeavesOutAMissingAwsErrorCode() {
    assertEquals(
        List.of("S3Exception", "S3Exception"),
        List.of(
            S3RealtimeRawDataConverter.causeName(S3Exception.builder().build()),
            S3RealtimeRawDataConverter.causeName(
                S3Exception.builder().awsErrorDetails(AwsErrorDetails.builder().build()).build())));
  }

  @Test
  void theBatchEstimateCountsArrayItems() {
    assertEquals(43L, S3RealtimeRawDataConverter.estimate(json("{\"a\":[\"xy\",1]}")));
  }

  @Test
  void aStopSeenBeforeAnOutcomeEndsTheObjectWithNothingEmitted() {
    stub("k", utf8("x"));
    TestEmitter emitter = new TestEmitter();
    // the stop is seen on entry to the second outcome, so that outcome is not counted
    emitter.stopAtCall = 4;

    stubbed(
            "json",
            false,
            new DecodeOutcome.Decoded(json("{\"a\":1}"), 1, new RawSpan(0, 7)),
            over(LimitKind.MAX_RECORD_BYTES, 1, 2))
        .emit(message("k"), context, emitter);

    assertEquals(List.of(), emitter.batches);
    assertEquals(List.of(), dlq);
    assertEquals(List.of(), List.copyOf(confirmed));
    verify(deserializeFailureCounter, never()).increase(anyMap());
  }

  /** Collects a parquet file in memory. */
  private record MemoryOutputFile(OutputStream out) implements OutputFile {
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

  @Test
  void theOtherLegacyEncodingsDecodeThroughTheConverter() {
    stub("o.json", utf8("{\"a\":1}"));
    stub("a.json", utf8("[{\"a\":1},{\"a\":2}]"));
    stub("t.txt", utf8("hello\nworld"));
    stub("x.xml", utf8("<r><a>1</a></r>"));
    assertEquals(
        List.of(
            List.of(json("{\"a\":1}")),
            List.of(json("{\"a\":1}"), json("{\"a\":2}")),
            List.of(json("{\"__raw__\":\"hello\\nworld\"}")),
            List.of(json("{\"a\":\"1\"}"))),
        List.of(
            emit(converter(EncodingType.JSON_OBJECT), message("o.json")).records(),
            emit(converter(EncodingType.JSON_ARRAY), message("a.json")).records(),
            emit(converter(EncodingType.TEXT), message("t.txt")).records(),
            emit(converter(EncodingType.XML), message("x.xml")).records()));
  }

  @Test
  void aMalformedDocumentEndsItsObjectAndTheGoodSiblingIsStillEmitted() {
    stub("bad.json", utf8("{bad"));
    stub("good.json", utf8("{\"a\":1}"));

    TestEmitter emitter =
        emit(converter(EncodingType.JSON_OBJECT), message("bad.json", "good.json"));

    assertEquals(List.of(json("{\"a\":1}")), emitter.records());
    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "MALFORMED json object: JsonParseException",
                malformedAction("json"),
                "object s3://b/bad.json version v1; 1 outcome; the document (bytes 0-4); the object ended",
                "node")),
        dlq);
  }

  @Test
  void anObjectWithoutRecordsCountsNoInputSize() {
    stub("bad.jsonl", utf8("this is definitely not json\n"));

    emit(converter(EncodingType.JSON_OBJECT_LINE), message("bad.jsonl"));

    verify(dataSizeCounter, never()).increase(anyLong(), anyMap());
  }

  @Test
  void aSpanlessFormatCountsTheStoredBytes() throws IOException {
    stub("x.yaml", utf8("--- {a: 1}\n--- {a: 2}\n"));

    emit(
        converter(
            decoding("{\"format\":{\"type\":\"yaml\",\"mode\":\"multi_document\"}}"),
            HEAP,
            4 * MIB),
        message("x.yaml"));

    verify(dataSizeCounter).increase(eq(22L), anyMap());
  }

  @Test
  void csvRowsWithDifferentColumnCountsShareTheFirstLine() {
    stub("x.csv", utf8("a,b,c\n1\n1,2\n1,2,3,4\n"));

    emit(converter(EncodingType.CSV), message("x.csv"));

    assertEquals(
        List.of(
            "MALFORMED csv record: MalformedCsvException",
            "MALFORMED csv record: MalformedCsvException",
            "MALFORMED csv record: MalformedCsvException"),
        dlq.stream().map(Entry::firstLine).toList());
  }

  @Test
  void corruptCompressionIsSummarized() {
    stub("x.jsonl.gz", concat(gzip(utf8("{\"a\":1}\n")), utf8("xx")));

    TestEmitter emitter = emit(converter(EncodingType.JSON_OBJECT_LINE), message("x.jsonl.gz"));

    assertEquals(List.of(json("{\"a\":1}")), emitter.records());
    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "CORRUPT_COMPRESSION json object: CorruptCompressionException",
                CORRUPT_ACTION,
                "object s3://b/x.jsonl.gz version v1; 1 outcome; recordIndex 2; the object ended at"
                    + " recordIndex 2",
                "node")),
        dlq);
  }

  @Test
  void anAccessDeniedDownloadNamesItsAwsCode() {
    when(s3Client.getObject(any(GetObjectRequest.class)))
        .thenThrow(
            S3Exception.builder()
                .statusCode(403)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build())
                .build());

    emit(converter(EncodingType.JSON_OBJECT_LINE), message("k.json"));

    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "READ_FAILURE s3 object: S3Exception AccessDenied",
                READ_ACTION,
                "object s3://b/k.json; 1 outcome; the object ended",
                "node")),
        dlq);
  }

  @Test
  void aDownstreamSummaryListsAtMost100Ranges() {
    StringBuilder lines = new StringBuilder();
    for (int i = 1; i <= 250; i++) {
      lines.append("{\"i\":").append(i).append("}\n");
    }
    stub("many.jsonl", utf8(lines.toString()));
    TestEmitter emitter = new TestEmitter();
    emitter.failEvenBatches = true;

    converter(new S3Decoding(EncodingType.JSON_OBJECT_LINE, null, null, false), HEAP, 1)
        .emit(message("many.jsonl"), context, emitter);

    assertEquals(250, emitter.batches.size());
    assertEquals(
        List.of(
            "object s3://b/many.jsonl version v1; 125 batches failed; recordIndex "
                + indexes(2, 200, 2)
                + ", and 25 more ranges"),
        dlq.stream().map(Entry::lastLine).toList());
  }

  @Test
  void halfAMillionNonAdjacentBytesLessFailuresGiveOneSummary() {
    StringBuilder yaml = new StringBuilder();
    for (int i = 0; i < 500_000; i++) {
      yaml.append("--- 1\n--- {a: 1}\n");
    }
    stub("x.yaml", utf8(yaml.toString()));
    // Counted, not recorded: a mock or a kept batch would hold half a million calls or records.
    AtomicLong records = new AtomicLong();
    AtomicLong failures = new AtomicLong();
    BatchEmittingConverter.Emitter emitter =
        new BatchEmittingConverter.Emitter() {
          @Override
          public void accept(List<RecordFleakData> batch) {
            records.addAndGet(batch.size());
          }

          @Override
          public boolean stopped() {
            return false;
          }
        };
    SourceExecutionContext<S3EventMessage> counting =
        new SourceExecutionContext<>(
            null, null, null, counter(null), counter(null), counter(failures), null);

    converter(
            decoding("{\"format\":{\"type\":\"yaml\",\"mode\":\"multi_document\"}}"), HEAP, 4 * MIB)
        .emit(message("x.yaml"), counting, emitter);

    assertEquals(List.of(500_000L, 500_000L), List.of(records.get(), failures.get()));
    assertEquals(
        List.of(
            "object s3://b/x.yaml version v1; 500000 outcomes; recordIndex "
                + indexes(1, 199, 2)
                + ", and 499900 more ranges"),
        dlq.stream().map(Entry::lastLine).toList());
  }

  @Test
  void aReadFailureAfterTheStopIsNotReported() {
    TestEmitter emitter = new TestEmitter();
    when(s3Client.getObject(any(GetObjectRequest.class)))
        .thenAnswer(
            invocation -> {
              emitter.stopped = true;
              throw new IllegalStateException("client closed");
            });

    converter(EncodingType.JSON_OBJECT_LINE).emit(message("k.json"), context, emitter);

    assertEquals(List.of(), dlq);
    verify(deserializeFailureCounter, never()).increase(anyMap());
  }

  /** A converter whose decoder gives one record and then fails to read the local copy. */
  private S3RealtimeRawDataConverter recordThenReadFailure(Runnable beforeFailing) {
    RecordDecoder failing =
        (input, limits) ->
            new RecordCursor() {
              private boolean first = true;

              @Override
              public DecodeOutcome next() throws SourceReadException {
                if (first) {
                  first = false;
                  return new DecodeOutcome.Decoded(json("{\"a\":1}"), 1, new RawSpan(0, 7));
                }
                beforeFailing.run();
                throw new SourceReadException("disk read failed", null);
              }

              @Override
              public void close() {}
            };
    return new S3RealtimeRawDataConverter(
        new S3RegionalClientProvider(region -> s3Client),
        new S3ObjectDecoder(failing, DecodeLimits.UNLIMITED, false),
        "json",
        MAX_OBJECT_SIZE,
        spoolDir,
        4 * MIB,
        confirmed,
        dlqWriter,
        new S3RealtimeRawDataEncoder(),
        "node",
        skippedObjectCounter);
  }

  @Test
  void aReadFailureMidObjectEmitsThePendingBatchAndIsSummarized() {
    stub("k.jsonl", utf8("{\"a\":1}\n"));
    TestEmitter emitter = new TestEmitter();

    recordThenReadFailure(() -> {}).emit(message("k.jsonl"), context, emitter);

    assertEquals(List.of(List.of(json("{\"a\":1}"))), emitter.batches);
    assertEquals(
        List.of("READ_FAILURE s3 object: SourceReadException"),
        dlq.stream().map(Entry::firstLine).toList());
  }

  @Test
  void aReadFailureAfterTheStopDoesNotEmitThePendingBatch() {
    stub("k.jsonl", utf8("{\"a\":1}\n"));
    TestEmitter emitter = new TestEmitter();

    recordThenReadFailure(() -> emitter.stopped = true).emit(message("k.jsonl"), context, emitter);

    assertEquals(List.of(), emitter.batches);
    assertEquals(List.of(), dlq);
  }

  @Test
  void aStopWhileThePendingBatchIsEmittedAfterAReadFailureWritesNoSummary() {
    stub("k.jsonl", utf8("{\"a\":1}\n"));
    TestEmitter emitter = new TestEmitter();
    emitter.stopAfterBatch = 0;

    recordThenReadFailure(() -> {}).emit(message("k.jsonl"), context, emitter);

    assertEquals(List.of(List.of(json("{\"a\":1}"))), emitter.batches);
    assertEquals(List.of(), dlq);
  }

  @Test
  void aDlqWriteThatFailsBecauseOfTheStopIsNotAnError() {
    stub("bad.jsonl", utf8("not json\n"));
    TestEmitter emitter = new TestEmitter();
    doAnswer(
            invocation -> {
              emitter.stopped = true;
              throw new IllegalStateException("BufferedWriter is closed");
            })
        .when(dlqWriter)
        .writeToDlq(anyLong(), any(), anyString(), anyString());

    assertDoesNotThrow(
        () ->
            converter(EncodingType.JSON_OBJECT_LINE).emit(message("bad.jsonl"), context, emitter));
  }

  @Test
  void aStopAfterTheLastRecordEmitsNothingMore() {
    stub("k.jsonl", utf8("{\"a\":1}\n"));
    TestEmitter emitter = new TestEmitter();
    // calls: before the object, at the record, after the record, then when the object ends
    emitter.stopAtCall = 4;

    converter(EncodingType.JSON_OBJECT_LINE).emit(message("k.jsonl"), context, emitter);

    assertEquals(List.of(), emitter.batches);
    verify(inputEventCounter, never()).increase(anyLong(), anyMap());
    verify(dataSizeCounter, never()).increase(anyLong(), anyMap());
  }

  @Test
  void aDownstreamFailureCausedByTheStopIsNotSummarized() {
    stub("k.jsonl", utf8("{\"a\":1}\n"));
    TestEmitter emitter = new TestEmitter();
    emitter.failBatch = 0;
    emitter.stopWhenFailing = true;

    converter(EncodingType.JSON_OBJECT_LINE).emit(message("k.jsonl"), context, emitter);

    assertEquals(List.of(), dlq);
  }

  @Test
  void aBadRecordOfExactly128KibIsDeadLetteredWhole() {
    String bad = "x".repeat(S3RealtimeRawDataConverter.MAX_RECORD_ENTRY_BYTES);
    stub("bad.jsonl", utf8(bad + "\n"));

    emit(converter(EncodingType.JSON_OBJECT_LINE), message("bad.jsonl"));

    assertEquals(
        List.of(
            new Entry(
                bad,
                "MALFORMED json record: JsonParseException",
                RECORD_ACTION,
                "object s3://b/bad.jsonl, recordIndex 1, bytes 0-" + bad.length(),
                "node")),
        dlq);
  }

  @Test
  void aFailedDlqWriteIsLoggedAndTheObjectGoesOn() {
    stub("bad.jsonl", utf8("not json\n{\"a\":1}\nalso bad\n"));
    doThrow(new IllegalStateException("dlq down"))
        .doAnswer(
            invocation -> {
              dlq.add(
                  new Entry("", invocation.getArgument(2), null, "", invocation.getArgument(3)));
              return null;
            })
        .when(dlqWriter)
        .writeToDlq(anyLong(), any(), anyString(), anyString());

    TestEmitter emitter = emit(converter(EncodingType.JSON_OBJECT_LINE), message("bad.jsonl"));

    assertEquals(List.of(json("{\"a\":1}")), emitter.records());
    assertEquals(
        List.of("MALFORMED json record: JsonParseException"),
        dlq.stream().map(e -> e.firstLine().lines().findFirst().orElse("")).toList());
  }

  @Test
  void entriesCarryTheErrorsStackTraceBetweenTheActionAndTheObject() {
    stub("bad.jsonl", utf8("{\"a\":1}\nnot json\n"));

    emit(converter(EncodingType.JSON_OBJECT_LINE), message("bad.jsonl"));

    List<String> lines = texts.getFirst();
    assertEquals(
        List.of(
            "MALFORMED json record: JsonParseException",
            RECORD_ACTION,
            "object s3://b/bad.jsonl, recordIndex 2, bytes 8-16"),
        List.of(lines.get(0), lines.get(1), lines.getLast()));
    List<String> trace = lines.subList(2, lines.size() - 1);
    assertTrue(
        trace.getFirst().startsWith("com.fasterxml.jackson.core.JsonParseException: "),
        trace.getFirst());
    assertTrue(trace.stream().anyMatch(l -> l.startsWith("\tat ")), String.join("\n", trace));
  }

  @Test
  void aMultilineEventPastTheRecordLimitPointsAtThePattern() {
    StringBuilder log = new StringBuilder();
    for (int i = 0; i < 2000; i++) {
      log.append("2024-01-01 line ").append(i).append('\n');
    }
    stub("app.log", utf8(log.toString()));

    emit(
        converter(
            decoding(
                "{\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"startPattern\":\"^\\\\[\"}}}"),
            MIB,
            4 * MIB),
        message("app.log"));

    long size = log.length() - 1;
    assertEquals(
        List.of(
            List.of(
                "OVERSIZED text object: maxRecordBytes",
                "Action: check the decoding's multiline startPattern or continuePattern (a pattern"
                    + " that does not match joins events until a limit); if events really are this"
                    + " large, this source sets maxRecordBytes to 1/128 of the task's heap, so give"
                    + " the task a heap of at least 128 times the event's size; "
                    + RETRY,
                "a multiline event reached " + size + " bytes, over maxRecordBytes 8192",
                "object s3://b/app.log version v1; 1 outcome; recordIndex 1 (bytes 0-"
                    + size
                    + ")")),
        texts);
  }

  @Test
  void anAvroObjectThatIsNotTheConfiguredCompressionPointsAtTheSetting() {
    stub("k.avro", utf8("not gzip at all"));

    emit(
        converter(
            decoding(
                "{\"compression\":[\"gzip\"],\"format\":{\"type\":\"avro\",\"container\":\"ocf\"}}"),
            HEAP,
            4 * MIB),
        message("k.avro"));

    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "CORRUPT_COMPRESSION avro object: CorruptCompressionException",
                CORRUPT_ACTION,
                "object s3://b/k.avro version v1; 1 outcome; recordIndex 1; the object ended at"
                    + " recordIndex 1",
                "node")),
        dlq);
  }

  @Test
  void aDamagedAvroObjectNamesBothCompressions() {
    stub("k", utf8("x"));

    emit(
        stubbed(
            "avro",
            false,
            new DecodeOutcome.Failed(
                ErrorKind.CORRUPT_COMPRESSION,
                new byte[0],
                1,
                2,
                null,
                new IllegalStateException("avro deflate block is corrupt"),
                false)),
        message("k"));

    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "CORRUPT_COMPRESSION avro object: IllegalStateException",
                CORRUPT_ACTION,
                "object s3://b/k version v1; 1 outcome; recordIndex 1-2",
                "node")),
        dlq);
  }

  @Test
  void aDeadLetteredLineLastSetsTheInputSize() {
    stub("k.jsonl", utf8("{\"a\":1}\nnot json\n"));

    emit(converter(EncodingType.JSON_OBJECT_LINE), message("k.jsonl"));

    verify(dataSizeCounter).increase(eq(16L), anyMap());
  }

  @Test
  void aReadFailureMidObjectSaysAfterWhichRecordItEnded() {
    stub("k.jsonl", utf8("{\"a\":1}\n"));

    recordThenReadFailure(() -> {}).emit(message("k.jsonl"), context, new TestEmitter());

    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "READ_FAILURE s3 object: SourceReadException",
                READ_ACTION,
                "object s3://b/k.jsonl; 1 outcome; the object ended after recordIndex 1",
                "node")),
        dlq);
  }

  @Test
  void aCsvHeaderFailureIsShownAsTheHeader() {
    stub("x.csv", utf8("a,a\n1,2\n"));

    emit(converter(EncodingType.CSV), message("x.csv"));

    assertEquals(
        List.of(
            new Entry(
                NOTIFICATION,
                "MALFORMED csv object: MalformedCsvException",
                malformedAction("csv"),
                "object s3://b/x.csv version v1; 1 outcome; the header; the object ended",
                "node")),
        dlq);
  }

  @Test
  void rangesMergeWhileKeptAndAreOnlyCountedAfterTheCap() {
    S3RealtimeRawDataConverter.IndexRanges ranges = new S3RealtimeRawDataConverter.IndexRanges();
    for (long i = 1; i <= 199; i += 2) {
      ranges.add(i, null);
    }
    ranges.add(200, null);
    ranges.add(202, null);
    ranges.add(203, null);
    ranges.add(205, null);
    assertEquals(
        "recordIndex " + indexes(1, 197, 2) + ", 199-200, and 2 more ranges", ranges.describe());
  }

  /** A counter that adds each call's count to {@code total}, or ignores it when that is null. */
  private static FleakCounter counter(AtomicLong total) {
    return new FleakCounter() {
      @Override
      public void increase(Map<String, String> additionalTags) {
        increase(1, additionalTags);
      }

      @Override
      public void increase(long n, Map<String, String> additionalTags) {
        if (total != null) {
          total.addAndGet(n);
        }
      }
    };
  }

  /** "first, first + step, ..., last". */
  private static String indexes(long first, long last, long step) {
    StringBuilder out = new StringBuilder();
    for (long i = first; i <= last; i += step) {
      out.append(i == first ? "" : ", ").append(i);
    }
    return out.toString();
  }

  /** A converter whose decoder gives these outcomes, then ends or fails to read the local copy. */
  private S3RealtimeRawDataConverter stubbed(
      String format, boolean thenReadFailure, DecodeOutcome... outcomes) {
    RecordDecoder stub =
        (input, limits) ->
            new RecordCursor() {
              private int next;

              @Override
              public DecodeOutcome next() throws SourceReadException {
                if (next < outcomes.length) {
                  return outcomes[next++];
                }
                if (thenReadFailure) {
                  throw new SourceReadException("disk read failed", null);
                }
                return null;
              }

              @Override
              public void close() {}
            };
    return new S3RealtimeRawDataConverter(
        new S3RegionalClientProvider(region -> s3Client),
        new S3ObjectDecoder(stub, DecodeLimits.UNLIMITED, false),
        format,
        MAX_OBJECT_SIZE,
        spoolDir,
        4 * MIB,
        confirmed,
        dlqWriter,
        new S3RealtimeRawDataEncoder(),
        "node",
        skippedObjectCounter);
  }

  private static DecodeOutcome.Oversized over(LimitKind limit, long value, long size) {
    return new DecodeOutcome.Oversized(1, 1, null, limit, value, size, new byte[0], false);
  }

  static Stream<Arguments> everyLimit() {
    String memory = "give the task more memory (xz may use half the heap, up to 128 MiB), or ";
    String pieces =
        "Action: write the file in smaller pieces (avro: smaller blocks; protobuf: smaller"
            + " messages; parquet: a smaller footer); no setting raises this limit; "
            + RETRY;
    return Stream.of(
        Arguments.of(
            "text",
            over(LimitKind.MAX_RECORD_BYTES, 8192, 9000),
            "OVERSIZED text object: maxRecordBytes",
            "Action: this source sets maxRecordBytes from the task's heap, so give the task more"
                + " memory"
                + LINE_FRAMING
                + RETRY,
            "a record reached 9000 bytes, over maxRecordBytes 8192"),
        Arguments.of(
            "json",
            over(LimitKind.MAX_DOCUMENT_BYTES, 4096, 5000),
            "OVERSIZED json object: maxDocumentBytes",
            "Action: this source sets maxDocumentBytes from the task's heap, so give the task more"
                + " memory, or write smaller documents or avro blocks; if one document holds many"
                + " records, decode it in a mode that reads them one at a time (text line, json"
                + " lines or array, xml elements, yaml multi_document, protobuf varint_delimited,"
                + " avro ocf; as encodingType: STRING_LINE, JSON_OBJECT_LINE or JSON_ARRAY); "
                + RETRY,
            "a document or block reached 5000 bytes, over maxDocumentBytes 4096"),
        Arguments.of(
            "avro",
            over(LimitKind.MAX_DOCUMENT_BYTES, 4096, 5000),
            "OVERSIZED avro object: maxDocumentBytes",
            "Action: this source sets maxDocumentBytes from the task's heap, so give the task more"
                + " memory, or write smaller documents or avro blocks; if one document holds many"
                + " records, decode it in a mode that reads them one at a time (text line, json"
                + " lines or array, xml elements, yaml multi_document, protobuf varint_delimited,"
                + " avro ocf; as encodingType: STRING_LINE, JSON_OBJECT_LINE or JSON_ARRAY); "
                + RETRY,
            "a document or block reached 5000 bytes, over maxDocumentBytes 4096"),
        Arguments.of(
            "json",
            over(LimitKind.MAX_CODEC_MEMORY_BYTES, 64 * MIB, 200 * MIB),
            "OVERSIZED json object: xz memory",
            "Action: "
                + memory
                + "recompress the object with a smaller xz dictionary (the presets -0 to -9 need"
                + " at most 65 MiB); "
                + RETRY,
            "xz needs " + 200 * MIB + " bytes of memory, over its limit of " + 64 * MIB),
        Arguments.of(
            "json",
            over(LimitKind.MAX_CODEC_MEMORY_BYTES, 128 * MIB, 200 * MIB),
            "OVERSIZED json object: xz memory",
            "Action: recompress the object with a smaller xz dictionary (the presets -0 to -9 need"
                + " at most 65 MiB); no setting raises this limit; "
                + RETRY,
            "xz needs " + 200 * MIB + " bytes of memory, over its limit of " + 128 * MIB),
        Arguments.of(
            "avro",
            over(LimitKind.AVRO_XZ_MEMORY, 128 * MIB, 200 * MIB),
            "OVERSIZED avro object: avro xz memory",
            "Action: rewrite the avro file with a lower xz level for its blocks (levels 0 to 9"
                + " need at most 65 MiB); no setting raises this limit; "
                + RETRY,
            "an avro block's xz data needs "
                + 200 * MIB
                + " bytes of memory, over its limit of "
                + 128 * MIB),
        Arguments.of(
            "parquet",
            over(LimitKind.HEAP_SHARE, 1000, 2000),
            "OVERSIZED parquet object: parquet heap share",
            "Action: give the task more memory, or write the file with smaller row groups or rows"
                + " (fewer row groups or columns if the footer is too large); "
                + RETRY,
            "parquet needs 2000 bytes, over its share of the heap, 1000"),
        Arguments.of(
            "text",
            over(LimitKind.MAX_LINES, 500, 9000),
            "OVERSIZED text object: maxLines",
            "Action: raise maxLines in the source's decoding, or fix its multiline pattern if"
                + " events run together; "
                + RETRY,
            "a multiline event has more than 500 lines (9000 bytes)"),
        Arguments.of(
            "json",
            over(LimitKind.ZSTD_WINDOW, 128 * MIB, 0),
            "OVERSIZED json object: zstd window",
            "Action: compress the zstd data (the object, or an avro file's zstandard blocks) without"
                + " long mode, or with a window of 128 MiB or less (--long=27); no setting raises"
                + " this limit; "
                + RETRY,
            "zstd data needs a window over " + 128 * MIB + " bytes"),
        Arguments.of(
            "avro",
            over(LimitKind.ZSTD_WINDOW, 128 * MIB, 0),
            "OVERSIZED avro object: zstd window",
            "Action: compress the zstd data (the object, or an avro file's zstandard blocks) without"
                + " long mode, or with a window of 128 MiB or less (--long=27); no setting raises"
                + " this limit; "
                + RETRY,
            "zstd data needs a window over " + 128 * MIB + " bytes"),
        Arguments.of(
            "json",
            over(LimitKind.NATIVE_MEMORY, 0, 0),
            "OVERSIZED json object: zstd native memory",
            "Action: the worker ran out of memory outside the Java heap: give the worker more"
                + " memory or run fewer tasks on it; if it was momentary, "
                + RETRY,
            "zstd could not allocate native memory"),
        Arguments.of(
            "protobuf",
            over(LimitKind.FORMAT_MAXIMUM, 2147483639, 2147483647),
            "OVERSIZED protobuf object: format maximum",
            pieces,
            "a protobuf message reached 2147483647 bytes, over the 2147483639 bytes the decoder"
                + " can hold in one piece"),
        Arguments.of(
            "avro",
            over(LimitKind.FORMAT_MAXIMUM, 2147483639, 2147483640),
            "OVERSIZED avro object: format maximum",
            pieces,
            "an avro block reached 2147483640 bytes, over the 2147483639 bytes the decoder can"
                + " hold in one piece"),
        Arguments.of(
            "parquet",
            over(LimitKind.FORMAT_MAXIMUM, 2147483639, 2147483647),
            "OVERSIZED parquet object: format maximum",
            pieces,
            "the parquet footer reached 2147483647 bytes, over the 2147483639 bytes the decoder"
                + " can hold in one piece"),
        Arguments.of(
            "json",
            over(LimitKind.FORMAT_MAXIMUM, 2147483639, 2147483647),
            "OVERSIZED json object: format maximum",
            pieces,
            "a unit reached 2147483647 bytes, over the 2147483639 bytes the decoder can hold in"
                + " one piece"));
  }

  @ParameterizedTest
  @MethodSource("everyLimit")
  void everyLimitSaysWhichItIsAndWhatToDo(
      String format,
      DecodeOutcome.Oversized oversized,
      String firstLine,
      String action,
      String detail) {
    stub("k", utf8("x"));

    emit(stubbed(format, false, oversized), message("k"));

    assertEquals(
        List.of(
            List.of(
                firstLine, action, detail, "object s3://b/k version v1; 1 outcome; recordIndex 1")),
        texts);
  }

  @Test
  void eachLimitHasItsOwnSummary() {
    stub("k", utf8("x"));

    emit(
        stubbed(
            "avro",
            false,
            over(LimitKind.MAX_DOCUMENT_BYTES, 4096, 5000),
            new DecodeOutcome.Oversized(
                2, 1, null, LimitKind.AVRO_XZ_MEMORY, 128 * MIB, 200 * MIB, new byte[0], false),
            new DecodeOutcome.Oversized(
                3, 1, null, LimitKind.MAX_DOCUMENT_BYTES, 4096, 6000, new byte[0], false)),
        message("k"));

    assertEquals(
        List.of("OVERSIZED avro object: maxDocumentBytes", "OVERSIZED avro object: avro xz memory"),
        dlq.stream().map(Entry::firstLine).toList());
    assertEquals(
        List.of(
            "object s3://b/k version v1; 2 outcomes; recordIndex 1, 3",
            "object s3://b/k version v1; 1 outcome; recordIndex 2"),
        dlq.stream().map(Entry::lastLine).toList());
  }

  @Test
  void anOutcomeCoversTheRecordsItCountsAndTheEndIsAfterTheLast() {
    stub("k", utf8("x"));
    IllegalStateException leftOver = new IllegalStateException("bytes left after its records");

    emit(
        stubbed(
            "avro",
            true,
            new DecodeOutcome.Oversized(
                2, 1000, null, LimitKind.MAX_DOCUMENT_BYTES, 4096, 5000, new byte[0], false),
            new DecodeOutcome.Failed(
                ErrorKind.MALFORMED, new byte[0], 1002, 0, null, leftOver, false)),
        message("k"));

    assertEquals(
        List.of(
            "object s3://b/k; 1 outcome; recordIndex 2-1001",
            "object s3://b/k; 1 outcome",
            "object s3://b/k; 1 outcome; the object ended after recordIndex 1001"),
        dlq.stream().map(Entry::lastLine).toList());
  }

  private static DecodingConfig decodingConfig(String json) {
    return JsonUtils.fromJsonString(json, DecodingConfig.class);
  }

  private static S3Decoding decoding(String json) {
    return new S3Decoding(null, null, decodingConfig(json), false);
  }

  private static RecordFleakData json(String json) {
    return (RecordFleakData) JsonUtils.loadFleakDataFromJsonString(json);
  }

  private static RecordFleakData record(Map<String, Object> payload) {
    return (RecordFleakData) FleakData.wrap(payload);
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] part : parts) {
      out.writeBytes(part);
    }
    return out.toByteArray();
  }

  private static byte[] gzip(byte[] bytes) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out.toByteArray();
  }
}
