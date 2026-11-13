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
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.filedecode.Decodings;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.Event;
import software.amazon.awssdk.services.s3.model.NotificationConfiguration;
import software.amazon.awssdk.services.s3.model.PutBucketNotificationConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.QueueConfiguration;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

@Testcontainers
class S3RealtimeSourceCommandIntegrationTest {

  @Container
  static LocalStackContainer LOCALSTACK =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.5"))
          .withServices(LocalStackContainer.Service.S3, LocalStackContainer.Service.SQS)
          .withStartupTimeout(Duration.ofMinutes(2));

  private static final String BUCKET = "evt-bucket";

  private S3Client s3;
  private SqsClient sqs;
  private String queueUrl;
  private ExecutorService executor;
  private SourceCommand command;
  private Future<?> execution;

  @BeforeEach
  void setUp() {
    s3 = newS3Client();
    sqs = newSqsClient();

    if (s3.listBuckets().buckets().stream().noneMatch(b -> b.name().equals(BUCKET))) {
      s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
    }
    queueUrl =
        sqs.createQueue(
                CreateQueueRequest.builder().queueName("evt-queue-" + UUID.randomUUID()).build())
            .queueUrl();
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() throws Exception {
    try {
      if (command != null) {
        command.requestStop();
        if (execution != null) {
          execution.get(30, TimeUnit.SECONDS);
        } else {
          command.terminate();
        }
      }
    } finally {
      executor.shutdownNow();
      try {
        assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS), "source worker must stop");
      } finally {
        s3.close();
        sqs.close();
      }
    }
  }

  @Test
  void endToEnd_realS3NotificationTriggersProcessing() throws Exception {
    // Wire a real S3 -> SQS event notification on the bucket, then create an object. LocalStack
    // (like real S3) emits the notification into the queue itself — we never call sendMessage here,
    // so anything the source consumes is a genuine S3 notification in S3's real wire format.
    enableS3Notifications();
    String key = "real/data.jsonl";
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET).key(key).build(),
        RequestBody.fromString("{\"msg\":\"a\"}\n{\"msg\":\"b\"}"));

    CountDownLatch beforeCommit = new CountDownLatch(1);
    CountDownLatch allowCommit = new CountDownLatch(1);
    CollectingAcceptor acceptor =
        new CollectingAcceptor() {
          @Override
          public void beforeCommit() {
            beforeCommit.countDown();
            try {
              assertTrue(allowCommit.await(30, TimeUnit.SECONDS), "test must release the commit");
            } catch (InterruptedException exception) {
              Thread.currentThread().interrupt();
              throw new AssertionError("interrupted before commit", exception);
            }
          }
        };
    CapturingDlqWriter dlq = new CapturingDlqWriter();
    runCommand(Decodings.JSON_LINES, 5, dlq, acceptor);

    try {
      assertTrue(beforeCommit.await(30, TimeUnit.SECONDS), "source must reach acknowledgement");
      assertEquals(
          List.of(record(Map.of("msg", "a")), record(Map.of("msg", "b"))), acceptor.records);
    } finally {
      allowCommit.countDown();
    }

    assertEquals(List.of(record(Map.of("msg", "a")), record(Map.of("msg", "b"))), acceptor.records);
    waitUntil(() -> approxMessages() == 0);
    assertEquals(0, approxMessages(), "processed notification should be deleted from the queue");
  }

  @Test
  void endToEnd_unparseableObjectDlqdAndAcknowledged() throws Exception {
    // The object exists but its only line is not JSON: that line is dead-lettered once with its
    // own bytes and the real error, and the message is acknowledged on the first attempt -- no
    // retry storm, no generic "exceeded maxRetries" entry.
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET).key("bad.jsonl").build(),
        RequestBody.fromString("this is definitely not json"));
    sendNotification(BUCKET, "bad.jsonl");

    CollectingAcceptor acceptor = new CollectingAcceptor();
    CapturingDlqWriter dlq = new CapturingDlqWriter();
    runCommand(Decodings.JSON_LINES, 5, dlq, acceptor);

    waitUntil(() -> dlq.captured.size() == 1);
    waitUntil(() -> approxMessages() == 0);

    assertTrue(
        acceptor.records.isEmpty(), "no records should be emitted for an unparseable object");
    assertEquals(1, dlq.captured.size(), "the bad line should be dead-lettered exactly once");
    DeadLetter entry = dlq.captured.getFirst();
    List<String> lines = entry.getErrorMessage().lines().toList();
    assertEquals(
        List.of(
            "this is definitely not json",
            "MALFORMED json record: JsonParseException",
            "object s3://" + BUCKET + "/bad.jsonl, recordIndex 1, bytes 0-27"),
        List.of(
            StandardCharsets.UTF_8.decode(entry.getValue()).toString(),
            lines.getFirst(),
            lines.getLast()),
        "the entry should hold the line's bytes and the real parse error");
  }

  @Test
  void endToEnd_downstreamFailureIsDlqd() throws Exception {
    // The object parses fine, but the downstream consumer throws. The framework's DLQ (restored)
    // must capture the record instead of silently dropping it.
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET).key("good.jsonl").build(),
        RequestBody.fromString("{\"msg\":\"a\"}"));
    sendNotification(BUCKET, "good.jsonl");

    ThrowingAcceptor acceptor = new ThrowingAcceptor();
    CapturingDlqWriter dlq = new CapturingDlqWriter();
    // maxRetries=2 / visibilityTimeout=0 so the cap drains the queue quickly while accept keeps
    // failing.
    runCommand(Decodings.JSON_LINES, 2, 0, dlq, acceptor);

    waitUntil(() -> !dlq.captured.isEmpty());
    waitUntil(() -> approxMessages() == 0);

    assertFalse(dlq.captured.isEmpty(), "downstream accept() failure must be captured in the DLQ");
  }

  // ---- helpers ----

  private static S3Client newS3Client() {
    return S3Client.builder()
        .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
        .credentialsProvider(credentials())
        .region(Region.of(LOCALSTACK.getRegion()))
        .forcePathStyle(true)
        .build();
  }

  private static SqsClient newSqsClient() {
    return SqsClient.builder()
        .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.SQS))
        .credentialsProvider(credentials())
        .region(Region.of(LOCALSTACK.getRegion()))
        .build();
  }

  private static StaticCredentialsProvider credentials() {
    return StaticCredentialsProvider.create(
        AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()));
  }

  private void runCommand(
      FormatConfig format, int maxRetries, CapturingDlqWriter dlq, SourceEventAcceptor acceptor)
      throws Exception {
    runCommand(format, maxRetries, 30, dlq, acceptor);
  }

  private void runCommand(
      FormatConfig format,
      int maxRetries,
      int visibilityTimeout,
      CapturingDlqWriter dlq,
      SourceEventAcceptor acceptor)
      throws Exception {
    JobContext job = TestUtils.buildJobContext(new HashMap<>());
    job.getOtherProperties()
        .put(
            "localstack",
            new HashMap<>(
                Map.of(
                    "username", LOCALSTACK.getAccessKey(), "password", LOCALSTACK.getSecretKey())));
    job.setDlqConfig(JobContext.S3DlqConfig.builder().build());
    AwsClientFactory factory = spy(new AwsClientFactory());
    doAnswer(invocation -> newSqsClient()).when(factory).createSqsClient(any(), any());
    S3RealtimeSourceCommand command =
        new S3RealtimeSourceCommand(
            "node",
            job,
            new JsonConfigParser<>(S3RealtimeSourceDto.Config.class),
            new S3RealtimeSourceConfigValidator(),
            factory);
    this.command = command;
    command.parseAndValidateArg(
        Map.of(
            "queueUrl",
            queueUrl,
            "regionStr",
            LOCALSTACK.getRegion(),
            "decoding",
            Decodings.autoCompressionConfig(format),
            "credentialId",
            "localstack",
            "s3EndpointOverride",
            LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
            "waitTimeSeconds",
            1,
            "maxRetries",
            maxRetries,
            "visibilityTimeoutSeconds",
            visibilityTimeout));
    MetricClientProvider metrics = mock(MetricClientProvider.class);
    when(metrics.counter(anyString(), anyMap())).thenReturn(mock(FleakCounter.class));
    try (var dlqFactory = mockStatic(DlqWriterFactory.class)) {
      dlqFactory
          .when(() -> DlqWriterFactory.createDlqWriter(job.getDlqConfig(), null))
          .thenReturn(dlq);
      command.initialize(metrics);
    }
    execution =
        executor.submit(
            () -> {
              try (var dlqFactory = mockStatic(DlqWriterFactory.class)) {
                dlqFactory
                    .when(() -> DlqWriterFactory.createSampleWriter(job.getDlqConfig(), null))
                    .thenReturn(mock(DlqWriter.class));
                command.execute("user", acceptor);
              }
              return null;
            });
  }

  private void sendNotification(String bucket, String key) {
    String body =
        "{\"Records\":[{\"eventName\":\"ObjectCreated:Put\",\"awsRegion\":\"us-east-1\",\"s3\":{\"bucket\":{\"name\":\""
            + bucket
            + "\"},\"object\":{\"key\":\""
            + key
            + "\"}}}]}";
    sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageBody(body).build());
  }

  private void enableS3Notifications() {
    String queueArn = queueArn();
    String policy =
        "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\","
            + "\"Principal\":{\"Service\":\"s3.amazonaws.com\"},"
            + "\"Action\":\"sqs:SendMessage\",\"Resource\":\""
            + queueArn
            + "\"}]}";
    sqs.setQueueAttributes(
        SetQueueAttributesRequest.builder()
            .queueUrl(queueUrl)
            .attributes(Map.of(QueueAttributeName.POLICY, policy))
            .build());
    s3.putBucketNotificationConfiguration(
        PutBucketNotificationConfigurationRequest.builder()
            .bucket(BUCKET)
            .notificationConfiguration(
                NotificationConfiguration.builder()
                    .queueConfigurations(
                        QueueConfiguration.builder()
                            .queueArn(queueArn)
                            .events(Event.S3_OBJECT_CREATED)
                            .build())
                    .build())
            .build());
  }

  private String queueArn() {
    return sqs.getQueueAttributes(
            GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN)
                .build())
        .attributes()
        .get(QueueAttributeName.QUEUE_ARN);
  }

  private int approxMessages() {
    GetQueueAttributesResponse attrs =
        sqs.getQueueAttributes(
            GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                .build());
    return Integer.parseInt(
            attrs.attributes().getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0"))
        + Integer.parseInt(
            attrs
                .attributes()
                .getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0"));
  }

  private static void waitUntil(java.util.function.BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(200);
    }
    fail("condition not met within timeout");
  }

  private static RecordFleakData record(Map<String, Object> payload) {
    return (RecordFleakData) FleakData.wrap(payload);
  }

  private static class CollectingAcceptor implements SourceEventAcceptor {
    final List<RecordFleakData> records = new CopyOnWriteArrayList<>();

    @Override
    public void accept(List<RecordFleakData> recordFleakData) {
      records.addAll(recordFleakData);
    }

    @Override
    public void terminate() {}
  }

  private static class ThrowingAcceptor implements SourceEventAcceptor {
    @Override
    public void accept(List<RecordFleakData> recordFleakData) {
      throw new RuntimeException("downstream boom");
    }

    @Override
    public void terminate() {}
  }

  private static class CapturingDlqWriter extends DlqWriter {
    final List<DeadLetter> captured = new CopyOnWriteArrayList<>();

    @Override
    public void open() {}

    @Override
    protected void doWrite(DeadLetter deadLetter) {
      captured.add(deadLetter);
    }

    @Override
    public void close() {}
  }
}
