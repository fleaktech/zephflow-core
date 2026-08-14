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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import io.fleak.zephflow.lib.commands.sink.ParquetBlobFileWriter;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.TextBlobFileWriter;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

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
      new MinIOContainer(DockerImageName.parse("minio/minio:latest")).withCommand("server /data");

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
        new TextBlobFileWriter(
            SerializerFactory.createSerializerFactory(EncodingType.JSON_OBJECT_LINE)
                .createSerializer(),
            EncodingType.JSON_OBJECT_LINE);

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
  void testScheduledFlushWritesToDlqWithNodeId() throws Exception {
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

    verify(mockDlqWriter)
        .writeToDlq(anyLong(), any(), contains("S3 upload error"), eq("s3-test-node"));
    dlqFlusher.close();
  }

  /**
   * Timer-driven and close-time flush results never travel back to SimpleSinkCommand, so the
   * flusher itself must report their failures — otherwise records that fail in a scheduled flush
   * are never counted in the sink error metric.
   */
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
}
