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
package io.fleak.zephflow.lib.commands.s3;

import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.encoding;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.execution.EffectOutcome;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.api.execution.ExecutionStoppedException;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import io.fleak.zephflow.lib.commands.sink.BoundedFlushException;
import io.fleak.zephflow.lib.commands.sink.ChronicleStoreForward;
import io.fleak.zephflow.lib.commands.sink.DestinationFailureException;
import io.fleak.zephflow.lib.commands.sink.ParquetBlobFileWriter;
import io.fleak.zephflow.lib.commands.sink.PassThroughMessagePreProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkStoreForward;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.encode.EncodingCompiler;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

@Testcontainers
class BatchS3FlusherTest {

  private static final String BUCKET_NAME = "test-batch-bucket";
  private static final String KEY_NAME = "test-events";

  private S3Client testS3Client;
  private BatchS3Flusher flusher;
  private AwsClientFactory awsClientFactory;
  private FleakCounter sinkOutputCounter;
  private FleakCounter outputSizeCounter;
  private FleakCounter sinkErrorCounter;

  @Container
  protected static MinIOContainer minioContainer =
      new MinIOContainer(TestUtils.MINIO_IMAGE).withCommand("server /data");

  @BeforeEach
  void setUp() {
    awsClientFactory = new AwsClientFactory();
    sinkOutputCounter = mock(FleakCounter.class);
    outputSizeCounter = mock(FleakCounter.class);
    sinkErrorCounter = mock(FleakCounter.class);
    testS3Client = createS3Client();
    testS3Client.createBucket(b -> b.bucket(BUCKET_NAME));
  }

  @AfterEach
  void tearDown() {
    if (flusher != null) {
      flusher.close();
      flusher = null;
    }
    if (testS3Client != null) {
      deleteAllObjectsInBucket();
      testS3Client.deleteBucket(b -> b.bucket(BUCKET_NAME));
      testS3Client.close();
    }
  }

  private S3Client createS3Client() {
    String endpoint = minioContainer.getS3URL();
    String accessKey = minioContainer.getUserName();
    String secretKey = minioContainer.getPassword();
    return awsClientFactory.createS3Client(
        "us-east-1", new UsernamePasswordCredential(accessKey, secretKey), endpoint);
  }

  private AwsClientFactory.S3TransferResources createS3TransferResources() {
    String endpoint = minioContainer.getS3URL();
    String accessKey = minioContainer.getUserName();
    String secretKey = minioContainer.getPassword();
    return awsClientFactory.createS3TransferResources(
        "us-east-1", new UsernamePasswordCredential(accessKey, secretKey), endpoint);
  }

  private BatchS3Flusher createFlusher(int batchSize, long flushIntervalMs) {
    return createFlusher(KEY_NAME, batchSize, flushIntervalMs);
  }

  private BatchS3Flusher createFlusher(String keyName, int batchSize, long flushIntervalMs) {
    AwsClientFactory.S3TransferResources s3TransferResources = createS3TransferResources();
    BlobFileWriter<RecordFleakData> fileWriter =
        EncodingCompiler.fileWriter(
            encoding("[]", "{\"type\":\"json\",\"mode\":\"lines\"}"),
            S3SinkConfigValidator.CAPABILITY);

    return new BatchS3Flusher(
        s3TransferResources,
        BUCKET_NAME,
        keyName,
        fileWriter,
        batchSize,
        flushIntervalMs,
        null,
        null,
        null,
        sinkOutputCounter,
        outputSizeCounter,
        sinkErrorCounter);
  }

  @Test
  void boundedUploadHasNoRetryAndAbortDoesNotUploadAgain() throws Exception {
    var manager = mock(software.amazon.awssdk.transfer.s3.S3TransferManager.class);
    var client = mock(software.amazon.awssdk.services.s3.S3AsyncClient.class);
    var upload = mock(software.amazon.awssdk.transfer.s3.model.FileUpload.class);
    var accepted = new java.util.concurrent.atomic.AtomicInteger();
    when(manager.uploadFile(any(software.amazon.awssdk.transfer.s3.model.UploadFileRequest.class)))
        .thenAnswer(
            call -> {
              accepted.incrementAndGet();
              return upload;
            });
    when(upload.completionFuture())
        .thenReturn(
            java.util.concurrent.CompletableFuture.failedFuture(
                new java.io.IOException("accepted but response lost")));
    var hooks =
        new io.fleak.zephflow.api.execution.ExecutionHooks(
            () -> {},
            new io.fleak.zephflow.api.execution.ExecutionHooks.Effects() {
              public long started() {
                return 1;
              }

              public void finished(
                  long id, io.fleak.zephflow.api.execution.EffectOutcome outcome) {}
            },
            Runnable::run);
    var fileWriter =
        EncodingCompiler.fileWriter(
            encoding("[]", "{\"type\":\"json\",\"mode\":\"lines\"}"),
            S3SinkConfigValidator.CAPABILITY);
    var sink =
        new BatchS3Flusher(
            new AwsClientFactory.S3TransferResources(client, manager),
            BUCKET_NAME,
            KEY_NAME,
            fileWriter,
            100,
            1,
            null,
            io.fleak.zephflow.api.JobContext.builder().executionHooks(hooks).build(),
            "s3",
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter);
    sink.initialize();
    var events = new SimpleSinkCommand.PreparedInputEvents<RecordFleakData>();
    var record = (RecordFleakData) FleakData.wrap(Map.of("id", 1));
    events.add(record, record);
    try {
      assertThrows(
          java.util.concurrent.CompletionException.class,
          () -> sink.flushBounded(events, Map.of(), hooks));
    } finally {
      sink.abort();
    }
    assertEquals(1, accepted.get());
    verify(manager).close();
    verify(client).close();
  }

  @Test
  void testFlushOnBatchSize() throws Exception {
    int batchSize = 5;
    flusher = createFlusher(batchSize, 60000);
    flusher.initialize();

    for (int i = 0; i < batchSize; i++) {
      Map<String, Object> data = new HashMap<>();
      data.put("id", i);
      data.put("name", "test" + i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(data);
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, record);
      flusher.flush(events, Map.of());
    }

    Thread.sleep(500);

    List<String> objectKeys = listS3Objects();
    assertEquals(1, objectKeys.size());
  }

  @Test
  void testFlushOnTimerInterval() throws Exception {
    int batchSize = 100;
    long flushIntervalMs = 1000;
    flusher = createFlusher(batchSize, flushIntervalMs);
    flusher.initialize();

    for (int i = 0; i < 3; i++) {
      Map<String, Object> data = new HashMap<>();
      data.put("id", i);
      data.put("name", "test" + i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(data);
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, record);
      flusher.flush(events, Map.of());
    }

    Thread.sleep(2000);

    List<String> objectKeys = listS3Objects();
    assertEquals(1, objectKeys.size());
  }

  /**
   * A timer-driven flush never returns its FlushResult to SimpleSinkCommand, so reportMetrics is
   * the only thing that can count it. Before reportMetrics was implemented here, these flushes
   * uploaded to S3 and reported nothing.
   */
  @Test
  void timerFlushReportsOutputSizeAndCount() throws Exception {
    flusher = createFlusher(100, 60000);
    flusher.initialize();

    for (int i = 0; i < 3; i++) {
      Map<String, Object> data = new HashMap<>();
      data.put("id", i);
      data.put("name", "test" + i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(data);
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, record);
      flusher.flush(events, Map.of());
    }

    // Buffer is below the batch size, so nothing has been reported yet.
    verify(outputSizeCounter, never()).increase(anyLong(), anyMap());

    flusher.executeScheduledFlush();

    verify(sinkOutputCounter).increase(eq(3L), anyMap());
    verify(outputSizeCounter).increase(longThat(size -> size > 0), anyMap());
  }

  /**
   * A batch-size-triggered flush returns its FlushResult to SimpleSinkCommand, which counts it. The
   * flusher must not also report it, or every inline flush is counted twice.
   */
  @Test
  void inlineFlushDoesNotDoubleReport() throws Exception {
    flusher = createFlusher(1, 60000);
    flusher.initialize();

    Map<String, Object> data = Map.of("id", 1, "name", "test");
    RecordFleakData record = (RecordFleakData) FleakData.wrap(data);
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(record, record);

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    // The size comes back on the result for SimpleSinkCommand to count...
    assertEquals(1, result.successCount());
    assertTrue(result.flushedDataSize() > 0);
    // ...and must not be reported a second time by the flusher itself.
    verify(outputSizeCounter, never()).increase(anyLong(), anyMap());
    verify(sinkOutputCounter, never()).increase(anyLong(), anyMap());
  }

  @Test
  void testS3ObjectKeyFormat() throws Exception {
    int batchSize = 1;
    flusher = createFlusher(batchSize, 60000);
    flusher.initialize();

    Map<String, Object> data = Map.of("id", 1, "name", "test");
    RecordFleakData record = (RecordFleakData) FleakData.wrap(data);
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(record, record);
    flusher.flush(events, Map.of());

    Thread.sleep(500);

    List<String> objectKeys = listS3Objects();
    assertEquals(1, objectKeys.size());

    String objectKey = objectKeys.get(0);
    String expectedPattern =
        KEY_NAME
            + "/year=\\d{4}/month=\\d{2}/day=\\d{2}/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\.jsonl";
    assertTrue(objectKey.matches(expectedPattern), "Key: " + objectKey);
  }

  @Test
  void testFlusherClose_flushesRemainingData() throws Exception {
    int batchSize = 100;
    flusher = createFlusher(batchSize, 60000);
    flusher.initialize();

    for (int i = 0; i < 3; i++) {
      Map<String, Object> data = new HashMap<>();
      data.put("id", i);
      data.put("name", "test" + i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(data);
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, record);
      flusher.flush(events, Map.of());
    }

    flusher.close();
    flusher = null;

    Thread.sleep(500);

    List<String> objectKeys = listS3Objects();
    assertEquals(1, objectKeys.size());
  }

  @Test
  void testParquetFileWriter() throws Exception {
    Map<String, Object> avroSchema =
        Map.of(
            "type", "record",
            "name", "TestRecord",
            "fields",
                List.of(
                    Map.of("name", "id", "type", "int"), Map.of("name", "name", "type", "string")));

    BlobFileWriter<RecordFleakData> parquetWriter = new ParquetBlobFileWriter(avroSchema);
    AwsClientFactory.S3TransferResources s3TransferResources = createS3TransferResources();

    flusher =
        new BatchS3Flusher(
            s3TransferResources,
            BUCKET_NAME,
            KEY_NAME,
            parquetWriter,
            5,
            60000,
            null,
            null,
            null,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter);
    flusher.initialize();

    for (int i = 0; i < 5; i++) {
      Map<String, Object> data = new HashMap<>();
      data.put("id", i);
      data.put("name", "test" + i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(data);
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, record);
      flusher.flush(events, Map.of());
    }

    Thread.sleep(500);

    List<String> objectKeys = listS3Objects();
    assertEquals(1, objectKeys.size());
    assertTrue(objectKeys.get(0).endsWith(".parquet"));
  }

  private static final String TEXT_LINE = "{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}";
  private static final String MISSING_RAW =
      "Record validation failed: record has no string __raw__ field to write as text";

  private BatchS3Flusher createTextFlusher(String compression, int batchSize) {
    return new BatchS3Flusher(
        createS3TransferResources(),
        BUCKET_NAME,
        KEY_NAME,
        EncodingCompiler.fileWriter(
            encoding(compression, TEXT_LINE), S3SinkConfigValidator.CAPABILITY),
        batchSize,
        60000,
        null,
        null,
        null,
        sinkOutputCounter,
        outputSizeCounter,
        sinkErrorCounter);
  }

  private static RecordFleakData raw(String text) {
    return (RecordFleakData) FleakData.wrap(Map.of("__raw__", text));
  }

  private static List<String> asaLines(int count) {
    List<String> lines = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      lines.add("%ASA-6-302013: Built inbound TCP connection " + i + " for outside:10.0.0.1/443");
    }
    return lines;
  }

  private static SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events(
      List<RecordFleakData> records) {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    records.forEach(record -> events.add(record, record));
    return events;
  }

  private byte[] readObject(String key) {
    return testS3Client.getObjectAsBytes(b -> b.bucket(BUCKET_NAME).key(key)).asByteArray();
  }

  @Test
  void textLinesUploadAsOneTxtObject() throws Exception {
    List<String> lines = asaLines(3);
    flusher = createTextFlusher("[]", 3);
    flusher.initialize();

    SimpleSinkCommand.FlushResult result =
        flusher.flush(events(lines.stream().map(BatchS3FlusherTest::raw).toList()), Map.of());

    String content = String.join("\n", lines) + "\n";
    List<String> keys = listS3Objects();
    assertEquals(
        List.of(
            new SimpleSinkCommand.FlushResult(3, content.length(), List.of()), 1, true, content),
        List.of(
            result,
            keys.size(),
            keys.getFirst().endsWith(".txt"),
            new String(readObject(keys.getFirst()), StandardCharsets.UTF_8)));
  }

  @Test
  void gzippedTextUploadsAsTxtGz() throws Exception {
    List<String> lines = asaLines(2);
    flusher = createTextFlusher("[\"gzip\"]", 2);
    flusher.initialize();

    flusher.flush(events(lines.stream().map(BatchS3FlusherTest::raw).toList()), Map.of());

    String key = listS3Objects().getFirst();
    try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(readObject(key)))) {
      assertEquals(
          List.of(true, String.join("\n", lines) + "\n"),
          List.of(key.endsWith(".txt.gz"), new String(in.readAllBytes(), StandardCharsets.UTF_8)));
    }
  }

  @Test
  void aRecordWithoutRawTextFailsAloneAndTheOthersUpload() throws Exception {
    List<String> lines = asaLines(9);
    List<RecordFleakData> records =
        new ArrayList<>(lines.stream().map(BatchS3FlusherTest::raw).toList());
    RecordFleakData bad = (RecordFleakData) FleakData.wrap(Map.of("message", "no raw"));
    records.add(4, bad);
    flusher = createTextFlusher("[]", 10);
    flusher.initialize();

    SimpleSinkCommand.FlushResult result = flusher.flush(events(records), Map.of());

    String content = String.join("\n", lines) + "\n";
    List<String> keys = listS3Objects();
    assertEquals(
        List.of(
            new SimpleSinkCommand.FlushResult(
                9, content.length(), List.of(new ErrorOutput(bad, MISSING_RAW))),
            1,
            content),
        List.of(
            result, keys.size(), new String(readObject(keys.getFirst()), StandardCharsets.UTF_8)));
  }

  @Test
  void aBatchWithNoRawTextUploadsNothing() throws Exception {
    List<RecordFleakData> records =
        List.of(
            (RecordFleakData) FleakData.wrap(Map.of("a", 1)),
            (RecordFleakData) FleakData.wrap(Map.of("__raw__", 2)));
    flusher = createTextFlusher("[]", 2);
    flusher.initialize();

    SimpleSinkCommand.FlushResult result = flusher.flush(events(records), Map.of());

    assertEquals(
        List.of(
            new SimpleSinkCommand.FlushResult(
                0,
                0,
                List.of(
                    new ErrorOutput(records.get(0), MISSING_RAW),
                    new ErrorOutput(records.get(1), MISSING_RAW))),
            List.of()),
        List.of(result, listS3Objects()));
  }

  private static final ExecutionHooks BOUNDED_HOOKS =
      new ExecutionHooks(
          () -> {},
          new ExecutionHooks.Effects() {
            public long started() {
              return 1;
            }

            public void finished(long id, EffectOutcome outcome) {}
          },
          Runnable::run);

  private SimpleSinkCommand.FlushResult flushBoundedText(
      S3TransferManager manager, List<RecordFleakData> records) throws Exception {
    BatchS3Flusher sink =
        new BatchS3Flusher(
            new AwsClientFactory.S3TransferResources(mock(S3AsyncClient.class), manager),
            BUCKET_NAME,
            KEY_NAME,
            EncodingCompiler.fileWriter(
                encoding("[]", TEXT_LINE), S3SinkConfigValidator.CAPABILITY),
            100,
            60000,
            null,
            JobContext.builder().executionHooks(BOUNDED_HOOKS).build(),
            "s3",
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter);
    sink.initialize();
    try {
      return sink.flushBounded(events(records), Map.of(), BOUNDED_HOOKS);
    } finally {
      sink.abort();
    }
  }

  private static List<RecordFleakData> nineLinesAndOneBad(RecordFleakData bad) {
    List<RecordFleakData> records =
        new ArrayList<>(asaLines(9).stream().map(BatchS3FlusherTest::raw).toList());
    records.add(bad);
    return records;
  }

  @Test
  void boundedRunReportsABadRecordAsADefiniteFailure() throws Exception {
    S3TransferManager manager = mock(S3TransferManager.class);
    FileUpload upload = mock(FileUpload.class);
    AtomicReference<String> uploaded = new AtomicReference<>();
    when(manager.uploadFile(any(UploadFileRequest.class)))
        .thenAnswer(
            call -> {
              UploadFileRequest request = call.getArgument(0);
              uploaded.set(Files.readString(request.source()));
              return upload;
            });
    when(upload.completionFuture()).thenReturn(CompletableFuture.completedFuture(null));
    RecordFleakData bad = (RecordFleakData) FleakData.wrap(Map.of("message", "no raw"));

    SimpleSinkCommand.FlushResult result = flushBoundedText(manager, nineLinesAndOneBad(bad));

    String content = String.join("\n", asaLines(9)) + "\n";
    assertEquals(
        List.of(
            new SimpleSinkCommand.FlushResult(
                9,
                content.length(),
                List.of(new ErrorOutput(bad, MISSING_RAW)),
                EffectOutcome.counted(9, 9, 1, 0, 1, "s3_record_validation")),
            content),
        List.of(result, uploaded.get()));
  }

  @Test
  void boundedStopDuringUploadKeepsTheRejectedRecordAsADefiniteFailure() throws Exception {
    S3TransferManager manager = mock(S3TransferManager.class);
    ExecutionStoppedException stop = new ExecutionStoppedException("run stopped");
    when(manager.uploadFile(any(UploadFileRequest.class))).thenThrow(stop);
    RecordFleakData bad = (RecordFleakData) FleakData.wrap(Map.of("message", "no raw"));

    BoundedFlushException thrown =
        assertThrows(
            BoundedFlushException.class, () -> flushBoundedText(manager, nineLinesAndOneBad(bad)));

    assertEquals(
        List.of(
            new SimpleSinkCommand.FlushResult(
                0,
                0,
                List.of(new ErrorOutput(bad, MISSING_RAW)),
                EffectOutcome.counted(9, 0, 1, 9, 1, "s3_record_validation")),
            stop),
        List.of(thrown.result(), thrown.getCause()));
  }

  @Test
  void boundedUploadFailureKeepsTheRejectedRecordAsADefiniteFailure() throws Exception {
    S3TransferManager manager = mock(S3TransferManager.class);
    FileUpload upload = mock(FileUpload.class);
    when(manager.uploadFile(any(UploadFileRequest.class))).thenReturn(upload);
    when(upload.completionFuture())
        .thenReturn(CompletableFuture.failedFuture(new IOException("upload down")));
    RecordFleakData bad = (RecordFleakData) FleakData.wrap(Map.of("message", "no raw"));
    List<RecordFleakData> records = nineLinesAndOneBad(bad);

    SimpleSinkCommand.FlushResult result = flushBoundedText(manager, records);

    String uploadError = new CompletionException(new IOException("upload down")).getMessage();
    List<ErrorOutput> errors = new ArrayList<>(List.of(new ErrorOutput(bad, MISSING_RAW)));
    records.subList(0, 9).forEach(record -> errors.add(new ErrorOutput(record, uploadError)));
    assertEquals(
        new SimpleSinkCommand.FlushResult(
            0, 0, errors, EffectOutcome.counted(9, 0, 1, 9, 1, "s3_record_validation")),
        result);
  }

  private BatchS3Flusher unboundedFlusher(
      S3TransferManager manager, BlobFileWriter<RecordFleakData> fileWriter) {
    BatchS3Flusher sink =
        new BatchS3Flusher(
            new AwsClientFactory.S3TransferResources(mock(S3AsyncClient.class), manager),
            BUCKET_NAME,
            KEY_NAME,
            fileWriter,
            100,
            60000,
            null,
            JobContext.builder().build(),
            "s3",
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter);
    sink.enableStoreForward(SinkStoreForward.noop(), new S3DestinationFailureClassifier());
    sink.initialize();
    return sink;
  }

  @Test
  void uploadFailureWithARejectedRecordThrowsADestinationFailureCarryingIt() throws Exception {
    S3TransferManager manager = mock(S3TransferManager.class);
    FileUpload upload = mock(FileUpload.class);
    when(manager.uploadFile(any(UploadFileRequest.class))).thenReturn(upload);
    IOException uploadDown = new IOException("upload down");
    when(upload.completionFuture()).thenReturn(CompletableFuture.failedFuture(uploadDown));
    RecordFleakData bad = (RecordFleakData) FleakData.wrap(Map.of("message", "no raw"));
    BatchS3Flusher sink =
        unboundedFlusher(
            manager,
            EncodingCompiler.fileWriter(
                encoding("[]", TEXT_LINE), S3SinkConfigValidator.CAPABILITY));

    try {
      DestinationFailureException thrown =
          assertThrows(
              DestinationFailureException.class,
              () -> sink.flushNow(events(nineLinesAndOneBad(bad)), Map.of()));
      assertEquals(
          List.of(List.of(new ErrorOutput(bad, MISSING_RAW)), uploadDown),
          List.of(thrown.getRecordErrors(), thrown.getCause().getCause()));
    } finally {
      sink.abort();
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void flushNowRethrowsATempFileFailureSoReplayKeepsTheRecords() throws Exception {
    BlobFileWriter<RecordFleakData> fileWriter = mock(BlobFileWriter.class);
    when(fileWriter.writeToTempFiles(anyList(), any()))
        .thenThrow(new IOException("No space left on device"));
    S3TransferManager manager = mock(S3TransferManager.class);
    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("id", 1));
    BatchS3Flusher sink = unboundedFlusher(manager, fileWriter);

    try {
      BatchS3Flusher.TempFileWriteException thrown =
          assertThrows(
              BatchS3Flusher.TempFileWriteException.class,
              () -> sink.flushNow(events(List.of(record)), Map.of()));
      assertEquals("No space left on device", thrown.getMessage());
      verify(manager, never()).uploadFile(any(UploadFileRequest.class));
    } finally {
      sink.abort();
    }
  }

  @Test
  void stoppingReplayDuringATextTempFileWriteKeepsTheChunkOnDisk(@TempDir Path storeDir)
      throws Exception {
    BlobFileWriter<RecordFleakData> text = textWriter();
    CountDownLatch writing = new CountDownLatch(1);
    BlobFileWriter<RecordFleakData> fileWriter =
        new DelegatingBlobFileWriter(text) {
          @Override
          public List<File> writeToTempFiles(List<RecordFleakData> records, Path dir)
              throws Exception {
            writing.countDown();
            try {
              new CountDownLatch(1).await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new ClosedByInterruptException();
            }
            return text.writeToTempFiles(records, dir);
          }
        };
    S3TransferManager manager = mock(S3TransferManager.class);
    BatchS3Flusher sink = unboundedFlusher(manager, fileWriter);
    ChronicleStoreForward store = replayStore(storeDir, sink);
    List<RecordFleakData> records = List.of(raw("line a"), raw("line b"));

    try {
      store.offer(records);
      assertTrue(writing.await(10, TimeUnit.SECONDS));
      store.stopReplay();
      store.close();

      assertEquals(records, replayedByANewStore(storeDir));
      verify(manager, never()).uploadFile(any(UploadFileRequest.class));
    } finally {
      store.close();
      sink.abort();
    }
  }

  @Test
  void stoppingReplayDuringAHangingUploadReturnsAndKeepsTheChunkOnDisk(@TempDir Path storeDir)
      throws Exception {
    S3TransferManager manager = mock(S3TransferManager.class);
    FileUpload upload = mock(FileUpload.class);
    CompletableFuture<CompletedFileUpload> hanging = new CompletableFuture<>();
    when(manager.uploadFile(any(UploadFileRequest.class))).thenReturn(upload);
    when(upload.completionFuture()).thenReturn(hanging);
    BatchS3Flusher sink = unboundedFlusher(manager, textWriter());
    ChronicleStoreForward store = replayStore(storeDir, sink);
    List<RecordFleakData> records = List.of(raw("line a"), raw("line b"));

    try {
      store.offer(records);
      verify(manager, timeout(10_000)).uploadFile(any(UploadFileRequest.class));
      CompletableFuture.runAsync(store::stopReplay).get(10, TimeUnit.SECONDS);
      store.close();

      assertEquals(records, replayedByANewStore(storeDir));
    } finally {
      hanging.cancel(true);
      store.close();
      sink.abort();
    }
  }

  @Test
  void replayRetriesAChunkAfterATempFileFailure(@TempDir Path storeDir) throws Exception {
    BlobFileWriter<RecordFleakData> text = textWriter();
    AtomicInteger writes = new AtomicInteger();
    BlobFileWriter<RecordFleakData> fileWriter =
        new DelegatingBlobFileWriter(text) {
          @Override
          public List<File> writeToTempFiles(List<RecordFleakData> records, Path dir)
              throws Exception {
            if (writes.incrementAndGet() == 1) {
              throw new IOException("No space left on device");
            }
            return text.writeToTempFiles(records, dir);
          }
        };
    S3TransferManager manager = mock(S3TransferManager.class);
    FileUpload upload = mock(FileUpload.class);
    when(manager.uploadFile(any(UploadFileRequest.class))).thenReturn(upload);
    when(upload.completionFuture())
        .thenReturn(CompletableFuture.completedFuture(mock(CompletedFileUpload.class)));
    BatchS3Flusher sink = unboundedFlusher(manager, fileWriter);
    FleakCounter dropped = mock(FleakCounter.class);
    ChronicleStoreForward store =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(storeDir, 1 << 20, 50, 100, "s3"),
            sink::keepsForReplay,
            null,
            null,
            dropped);
    sink.enableStoreForward(store, new S3DestinationFailureClassifier());
    store.start(replayTarget(sink));

    try {
      store.offer(List.of(raw("line a"), raw("line b")));
      verify(manager, timeout(10_000)).uploadFile(any(UploadFileRequest.class));
      await(() -> !store.isBuffering());

      assertEquals(2, writes.get());
      verify(dropped, never()).increase(anyLong(), anyMap());
    } finally {
      store.close();
      sink.abort();
    }
  }

  private static BlobFileWriter<RecordFleakData> textWriter() {
    return EncodingCompiler.fileWriter(encoding("[]", TEXT_LINE), S3SinkConfigValidator.CAPABILITY);
  }

  private ChronicleStoreForward replayStore(Path storeDir, BatchS3Flusher sink) {
    ChronicleStoreForward store =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(storeDir, 1 << 20, 50, 100, "s3"),
            new S3DestinationFailureClassifier()::isDestinationFailure,
            null,
            null,
            null);
    sink.enableStoreForward(store, new S3DestinationFailureClassifier());
    store.start(replayTarget(sink));
    return store;
  }

  private SinkStoreForward.ReplayTarget replayTarget(BatchS3Flusher sink) {
    return sink.replayTarget(
        new PassThroughMessagePreProcessor(),
        sinkOutputCounter,
        outputSizeCounter,
        sinkErrorCounter,
        mock(FleakCounter.class));
  }

  private static List<RecordFleakData> replayedByANewStore(Path storeDir) throws Exception {
    List<RecordFleakData> delivered = new CopyOnWriteArrayList<>();
    ChronicleStoreForward reopened =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(storeDir, 1 << 20, 50, 100, "s3"),
            t -> true,
            null,
            null,
            null);
    try {
      reopened.start(delivered::addAll);
      await(() -> !reopened.isBuffering());
      return delivered;
    } finally {
      reopened.close();
    }
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        fail("condition not met within 10s");
      }
      Thread.sleep(20);
    }
  }

  private static class DelegatingBlobFileWriter implements BlobFileWriter<RecordFleakData> {
    private final BlobFileWriter<RecordFleakData> delegate;

    DelegatingBlobFileWriter(BlobFileWriter<RecordFleakData> delegate) {
      this.delegate = delegate;
    }

    @Override
    public List<File> writeToTempFiles(List<RecordFleakData> records, Path dir) throws Exception {
      return delegate.writeToTempFiles(records, dir);
    }

    @Override
    public String getFileExtension() {
      return delegate.getFileExtension();
    }

    @Override
    public void validateRecord(RecordFleakData record) throws Exception {
      delegate.validateRecord(record);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void tempFileFailureOnATimerFlushIsCountedAndNotStored(@TempDir Path storeDir) throws Exception {
    BlobFileWriter<RecordFleakData> fileWriter = mock(BlobFileWriter.class);
    when(fileWriter.writeToTempFiles(anyList(), any()))
        .thenThrow(new IOException("No space left on device"));
    FleakCounter bufferedCounter = mock(FleakCounter.class);
    ChronicleStoreForward store =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(storeDir, 1 << 20, 100, 100, "s3"),
            new S3DestinationFailureClassifier()::isDestinationFailure,
            bufferedCounter,
            mock(FleakCounter.class),
            mock(FleakCounter.class));
    BatchS3Flusher sink =
        new BatchS3Flusher(
            new AwsClientFactory.S3TransferResources(
                mock(S3AsyncClient.class), mock(S3TransferManager.class)),
            BUCKET_NAME,
            KEY_NAME,
            fileWriter,
            100,
            60000,
            null,
            JobContext.builder().build(),
            "s3",
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter);
    sink.enableStoreForward(store, new S3DestinationFailureClassifier());
    sink.initialize();
    store.start(records -> {});
    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("id", 1));

    try {
      sink.flush(events(List.of(record)), Map.of());
      sink.executeScheduledFlush();
      assertFalse(store.isBuffering());
      verify(sinkErrorCounter).increase(1L, Map.of());
      verify(bufferedCounter, never()).increase(anyLong(), anyMap());
    } finally {
      sink.abort();
      store.close();
    }
  }

  private List<String> listS3Objects() {
    ListObjectsV2Request listRequest = ListObjectsV2Request.builder().bucket(BUCKET_NAME).build();
    ListObjectsV2Response listResponse = testS3Client.listObjectsV2(listRequest);
    return listResponse.contents().stream().map(S3Object::key).collect(Collectors.toList());
  }

  private void deleteAllObjectsInBucket() {
    ListObjectsV2Request listRequest =
        ListObjectsV2Request.builder().bucket(BatchS3FlusherTest.BUCKET_NAME).build();
    ListObjectsV2Response listResponse;
    do {
      listResponse = testS3Client.listObjectsV2(listRequest);
      for (S3Object s3Object : listResponse.contents()) {
        testS3Client.deleteObject(
            b -> b.bucket(BatchS3FlusherTest.BUCKET_NAME).key(s3Object.key()));
      }
      listRequest =
          listRequest.toBuilder().continuationToken(listResponse.nextContinuationToken()).build();
    } while (listResponse.isTruncated());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testScheduledFlushFailureIsCountedWithoutDeadLettering() throws Exception {
    DlqWriter mockDlqWriter = mock(DlqWriter.class);
    BlobFileWriter<RecordFleakData> mockFileWriter = mock(BlobFileWriter.class);
    when(mockFileWriter.getFileExtension()).thenReturn("jsonl");
    when(mockFileWriter.writeToTempFiles(anyList(), any()))
        .thenThrow(new RuntimeException("S3 upload error"));

    AwsClientFactory.S3TransferResources s3TransferResources = createS3TransferResources();
    BatchS3Flusher dlqFlusher =
        new BatchS3Flusher(
            s3TransferResources,
            BUCKET_NAME,
            KEY_NAME,
            mockFileWriter,
            100,
            60000,
            mockDlqWriter,
            null,
            "s3-test-node",
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter);
    dlqFlusher.initialize();

    Map<String, Object> data = Map.of("id", 1, "name", "test");
    RecordFleakData record = (RecordFleakData) FleakData.wrap(data);
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(record, record);
    dlqFlusher.flush(events, Map.of());

    dlqFlusher.executeScheduledFlush();

    verify(sinkErrorCounter).increase(1L, Map.of());
    verify(mockDlqWriter, never()).writeToDlq(anyLong(), any(), any(), any());
    dlqFlusher.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  void testScheduledFlushFailureReportsSinkErrors() throws Exception {
    BlobFileWriter<RecordFleakData> mockFileWriter = mock(BlobFileWriter.class);
    when(mockFileWriter.getFileExtension()).thenReturn("jsonl");
    when(mockFileWriter.writeToTempFiles(anyList(), any()))
        .thenThrow(new RuntimeException("S3 upload error"));

    AwsClientFactory.S3TransferResources s3TransferResources = createS3TransferResources();
    BatchS3Flusher failingFlusher =
        new BatchS3Flusher(
            s3TransferResources,
            BUCKET_NAME,
            KEY_NAME,
            mockFileWriter,
            100,
            60000,
            null,
            null,
            "s3-test-node",
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter);
    failingFlusher.initialize();

    for (int i = 0; i < 2; i++) {
      RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("id", i));
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, record);
      failingFlusher.flush(events, Map.of());
    }

    failingFlusher.executeScheduledFlush();

    verify(sinkErrorCounter).increase(eq(2L), anyMap());
    failingFlusher.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  void closeFlushFailureReportsSinkErrors() throws Exception {
    // A run with fewer records than batchSize writes them all at close; losing them there must
    // still count as sink errors, or the run summary reports a clean run.
    BlobFileWriter<RecordFleakData> mockFileWriter = mock(BlobFileWriter.class);
    when(mockFileWriter.getFileExtension()).thenReturn("jsonl");
    when(mockFileWriter.writeToTempFiles(anyList(), any()))
        .thenThrow(new RuntimeException("Access Denied"));

    BatchS3Flusher failingFlusher =
        new BatchS3Flusher(
            createS3TransferResources(),
            BUCKET_NAME,
            KEY_NAME,
            mockFileWriter,
            100,
            60000,
            null,
            null,
            "s3-test-node",
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter);
    failingFlusher.initialize();

    for (int i = 0; i < 3; i++) {
      RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("id", i));
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, record);
      failingFlusher.flush(events, Map.of());
    }

    failingFlusher.close();

    verify(sinkErrorCounter).increase(eq(3L), anyMap());
    verify(sinkErrorCounter).recordErrorSample(contains("Access Denied"));
  }
}
