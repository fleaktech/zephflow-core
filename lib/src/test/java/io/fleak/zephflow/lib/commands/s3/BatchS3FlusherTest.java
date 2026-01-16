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

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
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

  @Container
  protected static MinIOContainer minioContainer =
      new MinIOContainer(DockerImageName.parse("minio/minio:latest")).withCommand("server /data");

  @BeforeEach
  void setUp() {
    awsClientFactory = new AwsClientFactory();
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
      deleteAllObjectsInBucket(BUCKET_NAME);
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
    AwsClientFactory.S3TransferResources s3TransferResources = createS3TransferResources();
    S3FileWriter<RecordFleakData> fileWriter =
        new TextS3FileWriter(
            SerializerFactory.createSerializerFactory(EncodingType.JSON_OBJECT_LINE)
                .createSerializer(),
            EncodingType.JSON_OBJECT_LINE);

    return new BatchS3Flusher(
        s3TransferResources,
        BUCKET_NAME,
        KEY_NAME,
        fileWriter,
        batchSize,
        flushIntervalMs,
        null,
        null);
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

    S3FileWriter<RecordFleakData> parquetWriter = new ParquetS3FileWriter(avroSchema);
    AwsClientFactory.S3TransferResources s3TransferResources = createS3TransferResources();

    flusher =
        new BatchS3Flusher(
            s3TransferResources, BUCKET_NAME, KEY_NAME, parquetWriter, 5, 60000, null, null);
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

  private void deleteAllObjectsInBucket(String bucketName) {
    ListObjectsV2Request listRequest = ListObjectsV2Request.builder().bucket(bucketName).build();
    ListObjectsV2Response listResponse;
    do {
      listResponse = testS3Client.listObjectsV2(listRequest);
      for (S3Object s3Object : listResponse.contents()) {
        testS3Client.deleteObject(b -> b.bucket(bucketName).key(s3Object.key()));
      }
      listRequest =
          listRequest.toBuilder().continuationToken(listResponse.nextContinuationToken()).build();
    } while (listResponse.isTruncated());
  }
}
