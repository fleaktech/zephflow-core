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

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_DLQ_WRITE_ERROR_COUNT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

@Testcontainers
class S3DlqWriterStorageTest {

  private static final String KEY_PREFIX = "cluster-prefix/dev/pipeline-1/deployment-1/run-1";
  private static final Pattern BASE64_TOKEN = Pattern.compile("[A-Za-z0-9+/]{16,}={0,2}");
  private static final String REGION = "us-east-1";
  private static final int SEAWEEDFS_S3_PORT = 8333;
  private static final String SEAWEEDFS_IDENTITIES_FILE = "/etc/seaweedfs/s3.json";
  private static final String SEAWEEDFS_ACCESS_KEY = "access-" + UUID.randomUUID();
  private static final String SEAWEEDFS_SECRET_KEY = "x-" + UUID.randomUUID();

  @Container
  private static final MinIOContainer MINIO =
      new MinIOContainer(TestUtils.MINIO_IMAGE).withCommand("server /data");

  @Container
  private static final GenericContainer<?> SEAWEEDFS =
      new GenericContainer<>(DockerImageName.parse("chrislusf/seaweedfs:4.54"))
          .withCopyToContainer(
              Transferable.of(seaweedFsIdentities(SEAWEEDFS_ACCESS_KEY, SEAWEEDFS_SECRET_KEY)),
              SEAWEEDFS_IDENTITIES_FILE)
          .withCommand("mini", "-s3.config=" + SEAWEEDFS_IDENTITIES_FILE, "-master.telemetry=false")
          .withExposedPorts(SEAWEEDFS_S3_PORT)
          .waitingFor(Wait.forLogMessage(".*All enabled components are running.*", 1));

  record Store(String name, String endpoint, String accessKey, String secretKey) {
    @Override
    public String toString() {
      return name;
    }
  }

  record StoredDeadLetter(
      long processingTimestamp,
      String key,
      String value,
      Map<String, String> metadata,
      String errorMessage,
      String nodeId) {}

  static Stream<Store> stores() {
    return Stream.of(
        new Store("minio", MINIO.getS3URL(), MINIO.getUserName(), MINIO.getPassword()),
        new Store(
            "seaweedfs",
            "http://" + SEAWEEDFS.getHost() + ":" + SEAWEEDFS.getMappedPort(SEAWEEDFS_S3_PORT),
            SEAWEEDFS_ACCESS_KEY,
            SEAWEEDFS_SECRET_KEY));
  }

  @ParameterizedTest
  @MethodSource("stores")
  void writesDeadLettersThroughAnEndpointOverrideWithPathStyleAndStaticKeys(Store store)
      throws Exception {
    String bucket = "dlq-" + UUID.randomUUID();
    UsernamePasswordCredential credential =
        new UsernamePasswordCredential(store.accessKey(), store.secretKey());
    JobContext.S3DlqConfig config =
        JobContext.S3DlqConfig.builder()
            .region(REGION)
            .bucket(bucket)
            .batchSize(3)
            .flushIntervalMillis(60_000)
            .accessKeyId(store.accessKey())
            .secretAccessKey(store.secretKey())
            .s3EndpointOverride(store.endpoint())
            .build();
    MetricClientProvider metricClientProvider = mock(MetricClientProvider.class);
    FleakCounter unwrittenDeadLetterCounter = mock(FleakCounter.class);
    when(metricClientProvider.counter(METRIC_NAME_DLQ_WRITE_ERROR_COUNT, Map.of()))
        .thenReturn(unwrittenDeadLetterCounter);

    try (S3Client reader =
        new AwsClientFactory().createS3Client(REGION, credential, store.endpoint())) {
      reader.createBucket(b -> b.bucket(bucket));
      try (DlqWriter writer =
          DlqWriterFactory.createDlqWriter(config, KEY_PREFIX, metricClientProvider, Map.of())) {
        writer.open();
        for (int i = 0; i < 3; i++) {
          writer.writeToDlq(
              1_700_000_000_000L + i,
              new SerializedEvent(
                  ("key-" + i).getBytes(StandardCharsets.UTF_8),
                  ("value-" + i).getBytes(StandardCharsets.UTF_8),
                  Map.of("header", "h-" + i)),
              "error-" + i,
              "node-1");
        }
      }

      List<String> objectKeys =
          reader.listObjectsV2(b -> b.bucket(bucket)).contents().stream()
              .map(S3Object::key)
              .toList();
      assertEquals(1, objectKeys.size(), objectKeys.toString());
      assertTrue(
          objectKeys.getFirst().matches(KEY_PREFIX + "/dead-letters/dt=.+/hr=\\d{2}/.+_n3\\.avro"),
          objectKeys.getFirst());
      byte[] object =
          reader.getObjectAsBytes(b -> b.bucket(bucket).key(objectKeys.getFirst())).asByteArray();
      List<StoredDeadLetter> expected = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        expected.add(
            new StoredDeadLetter(
                1_700_000_000_000L + i,
                "key-" + i,
                "value-" + i,
                Map.of("header", "h-" + i),
                "error-" + i,
                "node-1"));
      }
      assertEquals(expected, readDeadLetters(object));
    }
    verify(metricClientProvider).counter(METRIC_NAME_DLQ_WRITE_ERROR_COUNT, Map.of());
    verifyNoInteractions(unwrittenDeadLetterCounter);
  }

  @Test
  void failedUploadLogsCountKeyAndErrorCodeButNoRecordContent() throws Exception {
    String recordValue = "record-content-" + UUID.randomUUID();
    JobContext.S3DlqConfig config =
        JobContext.S3DlqConfig.builder()
            .region("us-east-1")
            .bucket("missing-" + UUID.randomUUID())
            .batchSize(2)
            .flushIntervalMillis(60_000)
            .accessKeyId(MINIO.getUserName())
            .secretAccessKey(MINIO.getPassword())
            .s3EndpointOverride(MINIO.getS3URL())
            .build();

    MetricClientProvider metricClientProvider = mock(MetricClientProvider.class);
    FleakCounter unwrittenDeadLetterCounter = mock(FleakCounter.class);
    Map<String, String> metricTags = Map.of("node_id", "node-1");
    when(metricClientProvider.counter(METRIC_NAME_DLQ_WRITE_ERROR_COUNT, metricTags))
        .thenReturn(unwrittenDeadLetterCounter);

    List<CapturedEvent> events;
    try (RootLogCapture capture = new RootLogCapture();
        DlqWriter writer =
            DlqWriterFactory.createDlqWriter(
                config, KEY_PREFIX, metricClientProvider, metricTags)) {
      writer.open();
      for (int i = 0; i < 2; i++) {
        writer.writeToDlq(
            i,
            new SerializedEvent(
                ("key-" + i).getBytes(StandardCharsets.UTF_8),
                (recordValue + "-" + i).getBytes(StandardCharsets.UTF_8),
                Map.of("header", recordValue)),
            "error " + recordValue,
            "node-1");
      }
      events = capture.events();
    }

    List<CapturedEvent> writerEvents =
        events.stream().filter(e -> e.logger().equals(S3DlqWriter.class.getName())).toList();
    String objectKey = objectKeyOf(writerEvents.getFirst());
    assertTrue(
        objectKey.matches(
            KEY_PREFIX
                + "/dead-letters/dt=\\d{4}-\\d{2}-\\d{2}/hr=\\d{2}/deadletter-\\d+_[0-9a-f-]+_n2\\.avro"),
        objectKey);
    String failure = "NoSuchBucketException, status 404, error code NoSuchBucket";
    List<CapturedEvent> expectedWriterEvents = new ArrayList<>();
    for (int attempt = 1; attempt <= 3; attempt++) {
      expectedWriterEvents.add(
          new CapturedEvent(
              S3DlqWriter.class.getName(),
              Level.WARN,
              "Upload of "
                  + objectKey
                  + " failed (attempt "
                  + attempt
                  + "/3), retrying: "
                  + failure,
              null));
    }
    expectedWriterEvents.add(
        new CapturedEvent(
            S3DlqWriter.class.getName(),
            Level.ERROR,
            "Dropped 2 dead-letters records, object " + objectKey + " not written: " + failure,
            null));
    assertEquals(expectedWriterEvents, writerEvents);
    verify(unwrittenDeadLetterCounter).increase(2, Map.of());

    for (CapturedEvent event : events) {
      String text = event.message() + (event.thrown() == null ? "" : event.thrown());
      assertFalse(text.contains(recordValue), text);
      assertFalse(text.contains(MINIO.getPassword()), text);
      assertFalse(decodesToContent(text, recordValue), text);
    }
  }

  private static List<StoredDeadLetter> readDeadLetters(byte[] object) throws Exception {
    List<StoredDeadLetter> deadLetters = new ArrayList<>();
    try (DataFileReader<DeadLetter> reader =
        new DataFileReader<>(
            new SeekableByteArrayInput(object), new SpecificDatumReader<>(DeadLetter.class))) {
      while (reader.hasNext()) {
        DeadLetter deadLetter = reader.next();
        Map<String, String> metadata = new HashMap<>();
        deadLetter.getMetadata().forEach((k, v) -> metadata.put(k.toString(), v.toString()));
        deadLetters.add(
            new StoredDeadLetter(
                deadLetter.getProcessingTimestamp(),
                utf8(deadLetter.getKey()),
                utf8(deadLetter.getValue()),
                metadata,
                deadLetter.getErrorMessage().toString(),
                deadLetter.getNodeId().toString()));
      }
    }
    return deadLetters;
  }

  private static String utf8(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static String seaweedFsIdentities(String accessKey, String secretKey) {
    return JsonUtils.toJsonString(
        Map.of(
            "identities",
            List.of(
                Map.of(
                    "name",
                    "storage",
                    "credentials",
                    List.of(Map.of("accessKey", accessKey, "secretKey", secretKey)),
                    "actions",
                    List.of("Admin", "Read", "List", "Tagging", "Write")))));
  }

  private static String objectKeyOf(CapturedEvent event) {
    Matcher matcher = Pattern.compile("Upload of (\\S+) failed").matcher(event.message());
    assertTrue(matcher.find(), event.message());
    return matcher.group(1);
  }

  private static boolean decodesToContent(String text, String content) {
    Matcher matcher = BASE64_TOKEN.matcher(text);
    while (matcher.find()) {
      String token = matcher.group();
      String padded = token + "=".repeat((4 - token.length() % 4) % 4);
      try {
        if (new String(Base64.getDecoder().decode(padded), StandardCharsets.ISO_8859_1)
            .contains(content)) {
          return true;
        }
      } catch (IllegalArgumentException notBase64) {
        // not an encoded payload
      }
    }
    return false;
  }

  record CapturedEvent(String logger, Level level, String message, String thrown) {}

  /** Every event of every logger while open, with attached exceptions rendered in full. */
  private static final class RootLogCapture implements AutoCloseable {
    private final LoggerContext context = (LoggerContext) LogManager.getContext(false);
    private final List<CapturedEvent> events = new CopyOnWriteArrayList<>();
    private final AbstractAppender appender =
        new AbstractAppender(
            "root-log-capture",
            null,
            PatternLayout.createDefaultLayout(),
            false,
            Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            events.add(
                new CapturedEvent(
                    event.getLoggerName(),
                    event.getLevel(),
                    event.getMessage().getFormattedMessage(),
                    event.getThrown() == null
                        ? null
                        : ExceptionUtils.getStackTrace(event.getThrown())));
          }
        };

    RootLogCapture() {
      appender.start();
      context.getConfiguration().getRootLogger().addAppender(appender, Level.ALL, null);
      context.updateLoggers();
    }

    List<CapturedEvent> events() {
      return List.copyOf(events);
    }

    @Override
    public void close() {
      context.getConfiguration().getRootLogger().removeAppender(appender.getName());
      context.updateLoggers();
      appender.stop();
    }
  }
}
