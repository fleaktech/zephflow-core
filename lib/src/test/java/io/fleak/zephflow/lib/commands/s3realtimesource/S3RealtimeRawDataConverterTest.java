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
import io.fleak.zephflow.lib.commands.source.DeadLetterWriteException;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
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
   * A DLQ entry as a comparable value: the value as text, the first line and the last line of the
   * error text (the stack trace between them varies), and the node id.
   */
  record Entry(String value, String firstLine, String lastLine, String nodeId) {}

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
                "OVERSIZED text object",
                "object s3://b/big.txt version v1; 3 outcomes; recordIndex 1 (bytes 0-9000), 3-4 (bytes 9004-27005)",
                "node")),
        dlq);
    assertEquals(
        List.of(
            "OVERSIZED text object",
            "over the limit (maxRecordBytes 8192, maxDocumentBytes 16384, maxCodecMemoryBytes 524288), reached 9000 bytes",
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

    assertEquals(1, dlq.size());
    Entry entry = dlq.getFirst();
    assertEquals("OVERSIZED text object", entry.firstLine());
    assertTrue(
        entry
            .lastLine()
            .startsWith(
                "object s3://b/big.txt version v1; 300 outcomes; recordIndex 1 (bytes 0-9000), 3 (bytes 9004-18004)"),
        entry.lastLine());
    assertTrue(entry.lastLine().endsWith(", and 200 more ranges"), entry.lastLine());
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
            "CORRUPT_COMPRESSION json object: CorruptCompressionException",
            "object s3://b/x.jsonl.gz version v1; 1 outcome; recordIndex 2; the object ended at recordIndex 2"),
        List.of(dlq.getFirst().firstLine(), dlq.getFirst().lastLine()));
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
    assertEquals(1, dlq.size());
    String last = dlq.getFirst().lastLine();
    assertTrue(
        last.startsWith(
            "object s3://b/many.jsonl version v1; 125 batches failed; recordIndex 2, 4, 6,"),
        last);
    assertTrue(last.endsWith(", 200, and 25 more ranges"), last);
  }

  @Test
  void halfAMillionNonAdjacentBytesLessFailuresGiveOneSummary() {
    StringBuilder yaml = new StringBuilder();
    for (int i = 0; i < 500_000; i++) {
      yaml.append("--- 1\n--- {a: 1}\n");
    }
    stub("x.yaml", utf8(yaml.toString()));
    TestEmitter emitter = new TestEmitter();

    converter(
            decoding("{\"format\":{\"type\":\"yaml\",\"mode\":\"multi_document\"}}"), HEAP, 4 * MIB)
        .emit(message("x.yaml"), context, emitter);

    assertEquals(500_000, emitter.records().size());
    assertEquals(1, dlq.size());
    String last = dlq.getFirst().lastLine();
    assertTrue(
        last.startsWith("object s3://b/x.yaml version v1; 500000 outcomes; recordIndex 1, 3, 5,"),
        last);
    assertTrue(last.endsWith(", 199, and 499900 more ranges"), last);
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
                "object s3://b/bad.jsonl, recordIndex 1, bytes 0-" + bad.length(),
                "node")),
        dlq);
  }

  @Test
  void aFailedDlqWriteIsADeadLetterWriteExceptionAndNothingElseIsWritten() {
    stub("bad.jsonl", utf8("not json\n"));
    // only the first write fails, so a later summary written by mistake would be recorded
    doThrow(new IllegalStateException("dlq down"))
        .doAnswer(
            invocation -> {
              dlq.add(new Entry("", invocation.getArgument(2), "", invocation.getArgument(3)));
              return null;
            })
        .when(dlqWriter)
        .writeToDlq(anyLong(), any(), anyString(), anyString());

    DeadLetterWriteException e =
        assertThrows(
            DeadLetterWriteException.class,
            () -> emit(converter(EncodingType.JSON_OBJECT_LINE), message("bad.jsonl")));
    assertEquals("failed to write a DLQ entry for s3://b/bad.jsonl", e.getMessage());
    assertEquals(List.of(), dlq);
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
            "READ_FAILURE s3 object: SourceReadException",
            "object s3://b/k.jsonl; 1 outcome; the object ended after recordIndex 1"),
        List.of(dlq.getFirst().firstLine(), dlq.getFirst().lastLine()));
  }

  @Test
  void aCsvHeaderFailureIsShownAsTheHeader() {
    stub("x.csv", utf8("a,a\n1,2\n"));

    emit(converter(EncodingType.CSV), message("x.csv"));

    assertTrue(
        dlq.getFirst().lastLine().endsWith("; 1 outcome; the header; the object ended"),
        dlq.getFirst().lastLine());
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
    String described = ranges.describe();
    assertTrue(described.startsWith("recordIndex 1, 3, 5,"), described);
    assertTrue(described.endsWith(", 197, 199-200, and 2 more ranges"), described);
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
