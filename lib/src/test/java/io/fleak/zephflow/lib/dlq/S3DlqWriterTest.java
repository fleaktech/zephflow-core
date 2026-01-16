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
package io.fleak.zephflow.lib.dlq;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

/** Created by bolei on 11/9/24 */
@Testcontainers
public class S3DlqWriterTest {

  private static final String BUCKET_NAME = "test-dlq-bucket";

  private S3DlqWriter dlqWriter;

  @Container
  protected static MinIOContainer minioContainer =
      new MinIOContainer(DockerImageName.parse("minio/minio:latest")).withCommand("server /data");

  private void createAndOpenDlqWriter(int batchSize, long flushIntervalMillis) {
    String endpoint = minioContainer.getS3URL();
    String accessKey = minioContainer.getUserName();
    String secretKey = minioContainer.getPassword();
    dlqWriter =
        S3DlqWriter.createS3DlqWriter(
            Region.US_EAST_1.toString(),
            BUCKET_NAME,
            batchSize,
            flushIntervalMillis,
            new UsernamePasswordCredential(accessKey, secretKey),
            endpoint);
    dlqWriter.open();
    dlqWriter.s3Client.createBucket(b -> b.bucket(BUCKET_NAME));
  }

  @AfterEach
  public void afterEach() {
    if (dlqWriter != null) {
      deleteAllObjectsInBucket(dlqWriter.s3Client, BUCKET_NAME);
      dlqWriter.s3Client.deleteBucket(b -> b.bucket(BUCKET_NAME));
      dlqWriter.close();
    }
  }

  @Test
  void testDeadLettersFlushedOnBatchSize() throws InterruptedException {
    int batchSize = 5;
    long flushIntervalMillis = 10000; // Set high to avoid scheduled flush during test
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);
    // Write dead letters up to batch size
    writeDeadLetters(5);

    // Allow some time for flush to complete
    Thread.sleep(2000);

    // Verify that the objects are in S3 (MinIO)
    List<String> objectKeys = listS3Objects(dlqWriter.s3Client);
    assertEquals(1, objectKeys.size(), "Expected one object in S3 after batch size reached.");
  }

  @Test
  void testDeadLettersFlushedOnFlushInterval() throws InterruptedException {
    int batchSize = 5;
    long flushIntervalMillis = 1000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);

    writeDeadLetters(2);

    Thread.sleep(3000);

    List<String> objectKeys = listS3Objects(dlqWriter.s3Client);
    assertEquals(1, objectKeys.size(), "Expected one object in S3 after flush interval.");
  }

  @Test
  void testS3ObjectKeyFormat() throws InterruptedException {
    int batchSize = 1;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);
    writeDeadLetters(batchSize);
    Thread.sleep(2000);

    List<String> objectKeys = listS3Objects(dlqWriter.s3Client);
    assertEquals(1, objectKeys.size(), "Expected one object in S3.");

    String objectKey = objectKeys.get(0);
    String regex = "\\d{4}/\\d{2}/\\d{2}/\\d{2}/dead-letters-\\d+\\.avro";
    assertTrue(objectKey.matches(regex), "Object key does not match expected format.");
  }

  @Test
  void testUploadedDeadLettersContent() throws IOException, InterruptedException {
    int batchSize = 3;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);
    List<SerializedEvent> writtenDeadLetters = new ArrayList<>();
    writeDeadLetters(batchSize, writtenDeadLetters);

    Thread.sleep(2000);

    List<String> objectKeys = listS3Objects(dlqWriter.s3Client);
    assertEquals(1, objectKeys.size(), "Expected one object in S3 after batch size reached.");

    List<SerializedEvent> readDeadLetters =
        readDeadLettersFromS3(dlqWriter.s3Client, objectKeys.get(0));

    assertEquals(
        writtenDeadLetters.size(), readDeadLetters.size(), "Mismatch in number of dead letters.");

    for (int i = 0; i < batchSize; i++) {
      SerializedEvent written = writtenDeadLetters.get(i);
      SerializedEvent read = readDeadLetters.get(i);
      assertArrayEquals(written.key(), read.key(), "Mismatch in dead letter key.");
      assertArrayEquals(written.value(), read.value(), "Mismatch in dead letter value.");
      assertEquals(written.metadata(), read.metadata(), "Mismatch in dead letter metadata.");
    }
  }

  private List<String> listS3Objects(S3Client s3Client) {
    ListObjectsV2Request listRequest = ListObjectsV2Request.builder().bucket(BUCKET_NAME).build();
    ListObjectsV2Response listResponse = s3Client.listObjectsV2(listRequest);
    return listResponse.contents().stream().map(S3Object::key).collect(Collectors.toList());
  }

  private void writeDeadLetters(int count) {
    writeDeadLetters(count, null);
  }

  private void writeDeadLetters(int count, List<SerializedEvent> writtenDeadLetters) {
    for (int i = 0; i < count; i++) {
      SerializedEvent serializedEvent =
          i == 0
              ? new SerializedEvent(null, null, null)
              : new SerializedEvent(
                  ("key-" + i).getBytes(),
                  ("value-" + i).getBytes(),
                  Map.of("metadata_value", "metadata_value"));
      dlqWriter.writeToDlq(i, serializedEvent, "msg" + i);
      if (writtenDeadLetters != null) {
        writtenDeadLetters.add(serializedEvent);
      }
    }
  }

  private List<SerializedEvent> readDeadLettersFromS3(S3Client s3Client, String objectKey)
      throws IOException {
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(BUCKET_NAME).key(objectKey).build();

    ResponseBytes<GetObjectResponse> s3ObjectBytes = s3Client.getObjectAsBytes(getObjectRequest);

    byte[] data = s3ObjectBytes.asByteArray();

    SpecificDatumReader<DeadLetter> datumReader = new SpecificDatumReader<>(DeadLetter.class);
    try (SeekableByteArrayInput seekableByteArrayInput = new SeekableByteArrayInput(data);
        DataFileReader<DeadLetter> dataFileReader =
            new DataFileReader<>(seekableByteArrayInput, datumReader)) {
      List<SerializedEvent> deadLetters = new ArrayList<>();
      while (dataFileReader.hasNext()) {
        DeadLetter deadLetter = dataFileReader.next();
        deadLetters.add(
            new SerializedEvent(
                Optional.ofNullable(deadLetter.getKey()).map(ByteBuffer::array).orElse(null),
                Optional.ofNullable(deadLetter.getValue()).map(ByteBuffer::array).orElse(null),
                deadLetter.getMetadata()));
      }
      return deadLetters;
    }
  }

  public static void deleteAllObjectsInBucket(S3Client s3Client, String bucketName) {
    ListObjectsV2Request listRequest = ListObjectsV2Request.builder().bucket(bucketName).build();

    ListObjectsV2Response listResponse;
    do {
      listResponse = s3Client.listObjectsV2(listRequest);

      // Delete objects one by one instead of in bulk
      for (S3Object s3Object : listResponse.contents()) {
        DeleteObjectRequest deleteRequest =
            DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build();

        s3Client.deleteObject(deleteRequest);
      }

      listRequest =
          listRequest.toBuilder().continuationToken(listResponse.nextContinuationToken()).build();
    } while (listResponse.isTruncated());
  }
}
