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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class GcsDlqWriterTest {

  private static final String BUCKET_NAME = "test-dlq-bucket";
  private static final String TEST_KEY_PREFIX = "test-env/pipeline-1/deployment-1/run-1";
  private static final int FAKE_GCS_PORT = 4443;

  private GcsDlqWriter dlqWriter;
  private Storage gcsClient;

  @Container
  protected static GenericContainer<?> fakeGcsServer =
      new GenericContainer<>(DockerImageName.parse("fsouza/fake-gcs-server:latest"))
          .withExposedPorts(FAKE_GCS_PORT)
          .withCreateContainerCmdModifier(
              cmd ->
                  cmd.withEntrypoint(
                      "/bin/fake-gcs-server",
                      "-scheme",
                      "http",
                      "-port",
                      String.valueOf(FAKE_GCS_PORT)));

  private static String getGcsEndpoint() {
    return String.format(
        "http://%s:%d", fakeGcsServer.getHost(), fakeGcsServer.getMappedPort(FAKE_GCS_PORT));
  }

  private Storage createGcsClient() {
    return StorageOptions.newBuilder()
        .setHost(getGcsEndpoint())
        .setProjectId("test-project")
        .build()
        .getService();
  }

  private void createAndOpenDlqWriter(int batchSize, long flushIntervalMillis) {
    createAndOpenDlqWriter(batchSize, flushIntervalMillis, TEST_KEY_PREFIX);
  }

  private void createAndOpenDlqWriter(int batchSize, long flushIntervalMillis, String keyPrefix) {
    gcsClient = createGcsClient();
    gcsClient.create(com.google.cloud.storage.BucketInfo.of(BUCKET_NAME));

    Storage writerClient = createGcsClient();
    dlqWriter =
        new GcsDlqWriter(
            writerClient,
            BUCKET_NAME,
            batchSize,
            flushIntervalMillis,
            keyPrefix,
            "dead-letters",
            "deadletter");
    dlqWriter.open();
  }

  private void createAndOpenSampleWriter(
      int batchSize, long flushIntervalMillis, String keyPrefix) {
    gcsClient = createGcsClient();
    gcsClient.create(com.google.cloud.storage.BucketInfo.of(BUCKET_NAME));

    Storage writerClient = createGcsClient();
    dlqWriter =
        new GcsDlqWriter(
            writerClient,
            BUCKET_NAME,
            batchSize,
            flushIntervalMillis,
            keyPrefix,
            "raw-data-samples",
            "sample");
    dlqWriter.open();
  }

  @AfterEach
  public void afterEach() {
    if (dlqWriter != null) {
      dlqWriter.close();
    }
    if (gcsClient != null) {
      deleteAllObjectsInBucket();
      try {
        gcsClient.delete(BUCKET_NAME);
      } catch (Exception e) {
        // ignore
      }
      try {
        gcsClient.close();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  @Test
  void testDeadLettersFlushedOnBatchSize() throws InterruptedException {
    int batchSize = 5;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);

    writeDeadLetters(5);
    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size(), "Expected one object in GCS after batch size reached.");
  }

  @Test
  void testDeadLettersFlushedOnFlushInterval() throws InterruptedException {
    int batchSize = 5;
    long flushIntervalMillis = 1000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);

    writeDeadLetters(2);
    Thread.sleep(3000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size(), "Expected one object in GCS after flush interval.");
  }

  @Test
  void testGcsObjectKeyFormat() throws InterruptedException {
    int batchSize = 1;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);
    writeDeadLetters(batchSize);
    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size(), "Expected one object in GCS.");

    String objectName = objectNames.get(0);
    String regex =
        TEST_KEY_PREFIX
            + "/dead-letters/dt=\\d{4}-\\d{2}-\\d{2}/hr=\\d{2}/deadletter-\\d+_[0-9a-f\\-]+\\.avro";
    assertTrue(
        objectName.matches(regex), "Object key does not match expected format: " + objectName);
  }

  @Test
  void testGcsObjectKeyFormatWithNullPrefix() throws InterruptedException {
    int batchSize = 1;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis, null);
    writeDeadLetters(batchSize);
    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size());

    String objectName = objectNames.get(0);
    String regex =
        "dead-letters/dt=\\d{4}-\\d{2}-\\d{2}/hr=\\d{2}/deadletter-\\d+_[0-9a-f\\-]+\\.avro";
    assertTrue(
        objectName.matches(regex), "Object key does not match expected format: " + objectName);
  }

  @Test
  void testSampleWriterGcsObjectKeyFormat() throws InterruptedException {
    int batchSize = 1;
    long flushIntervalMillis = 10000;
    createAndOpenSampleWriter(batchSize, flushIntervalMillis, TEST_KEY_PREFIX);
    writeDeadLetters(batchSize);
    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size(), "Expected one object in GCS.");

    String objectName = objectNames.get(0);
    String regex =
        TEST_KEY_PREFIX
            + "/raw-data-samples/dt=\\d{4}-\\d{2}-\\d{2}/hr=\\d{2}/sample-\\d+_[0-9a-f\\-]+\\.avro";
    assertTrue(
        objectName.matches(regex), "Object key does not match expected format: " + objectName);
  }

  @Test
  void testSampleWriterGcsObjectKeyFormatWithNullPrefix() throws InterruptedException {
    int batchSize = 1;
    long flushIntervalMillis = 10000;
    createAndOpenSampleWriter(batchSize, flushIntervalMillis, null);
    writeDeadLetters(batchSize);
    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size());

    String objectName = objectNames.get(0);
    String regex =
        "raw-data-samples/dt=\\d{4}-\\d{2}-\\d{2}/hr=\\d{2}/sample-\\d+_[0-9a-f\\-]+\\.avro";
    assertTrue(
        objectName.matches(regex), "Object key does not match expected format: " + objectName);
  }

  @Test
  void testUploadedDeadLettersContent() throws IOException, InterruptedException {
    int batchSize = 3;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);
    List<SerializedEvent> writtenDeadLetters = new ArrayList<>();
    writeDeadLetters(batchSize, writtenDeadLetters);

    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size(), "Expected one object in GCS after batch size reached.");

    List<SerializedEvent> readDeadLetters = readDeadLettersFromGcs(objectNames.get(0));

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

  @Test
  void testUploadedDeadLettersContainNodeId() throws IOException, InterruptedException {
    int batchSize = 2;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);
    writeDeadLetters(batchSize);

    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size());

    List<DeadLetter> deadLetters = readRawDeadLettersFromGcs(objectNames.get(0));
    for (DeadLetter dl : deadLetters) {
      assertEquals("test-node", dl.getNodeId().toString());
    }
  }

  private List<String> listGcsObjects() {
    List<String> names = new ArrayList<>();
    for (Blob blob : gcsClient.list(BUCKET_NAME).iterateAll()) {
      names.add(blob.getName());
    }
    return names;
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
      dlqWriter.writeToDlq(i, serializedEvent, "msg" + i, "test-node");
      if (writtenDeadLetters != null) {
        writtenDeadLetters.add(serializedEvent);
      }
    }
  }

  private List<DeadLetter> readRawDeadLettersFromGcs(String objectName) throws IOException {
    byte[] data = gcsClient.readAllBytes(BUCKET_NAME, objectName);
    SpecificDatumReader<DeadLetter> datumReader = new SpecificDatumReader<>(DeadLetter.class);
    try (SeekableByteArrayInput input = new SeekableByteArrayInput(data);
        DataFileReader<DeadLetter> reader = new DataFileReader<>(input, datumReader)) {
      List<DeadLetter> deadLetters = new ArrayList<>();
      while (reader.hasNext()) {
        deadLetters.add(reader.next());
      }
      return deadLetters;
    }
  }

  private List<SerializedEvent> readDeadLettersFromGcs(String objectName) throws IOException {
    byte[] data = gcsClient.readAllBytes(BUCKET_NAME, objectName);
    SpecificDatumReader<DeadLetter> datumReader = new SpecificDatumReader<>(DeadLetter.class);
    try (SeekableByteArrayInput input = new SeekableByteArrayInput(data);
        DataFileReader<DeadLetter> reader = new DataFileReader<>(input, datumReader)) {
      List<SerializedEvent> deadLetters = new ArrayList<>();
      while (reader.hasNext()) {
        DeadLetter deadLetter = reader.next();
        deadLetters.add(
            new SerializedEvent(
                Optional.ofNullable(deadLetter.getKey()).map(ByteBuffer::array).orElse(null),
                Optional.ofNullable(deadLetter.getValue()).map(ByteBuffer::array).orElse(null),
                deadLetter.getMetadata()));
      }
      return deadLetters;
    }
  }

  private void deleteAllObjectsInBucket() {
    try {
      for (Blob blob : gcsClient.list(BUCKET_NAME).iterateAll()) {
        gcsClient.delete(blob.getBlobId());
      }
    } catch (Exception e) {
      // ignore cleanup errors
    }
  }
}
