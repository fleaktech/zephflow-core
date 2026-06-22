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
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
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
  void tearDown() {
    executor.shutdownNow();
    s3.close();
    sqs.close();
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

    CollectingAcceptor acceptor = new CollectingAcceptor();
    CapturingDlqWriter dlq = new CapturingDlqWriter();
    SourceCommand command = runCommand(EncodingType.JSON_OBJECT_LINE, 5, dlq, acceptor);

    waitUntil(() -> acceptor.records.size() >= 2);
    command.terminate();

    assertEquals(List.of(record(Map.of("msg", "a")), record(Map.of("msg", "b"))), acceptor.records);
    waitUntil(() -> approxMessages() == 0);
    assertEquals(0, approxMessages(), "processed notification should be deleted from the queue");
  }

  @Test
  void endToEnd_unparseableObjectIsRetryCappedAndDeadLettered() throws Exception {
    // The object exists but cannot be parsed as JSON, so every attempt fails -> never
    // committed/deleted by the converter. SQS redelivers it (real ApproximateReceiveCount
    // increments) until the in-app cap fires, which dead-letters and deletes it. Verifies the cap
    // works end to end. (A *missing* object is instead skipped gracefully, covered by a unit test.)
    s3.putObject(
        PutObjectRequest.builder().bucket(BUCKET).key("bad.jsonl").build(),
        RequestBody.fromString("this is definitely not json"));
    sendNotification(BUCKET, "bad.jsonl");

    CollectingAcceptor acceptor = new CollectingAcceptor();
    CapturingDlqWriter dlq = new CapturingDlqWriter();
    // maxRetries=2, visibilityTimeout=0 so the poison message reappears immediately.
    SourceCommand command = runCommand(EncodingType.JSON_OBJECT_LINE, 2, 0, dlq, acceptor);

    waitUntil(() -> approxMessages() == 0);
    command.terminate();

    assertTrue(
        acceptor.records.isEmpty(), "no records should be emitted for an unparseable object");
    assertTrue(
        dlq.captured.stream().anyMatch(d -> d.getErrorMessage().contains("exceeded maxRetries")),
        "DLQ should contain the retry-cap dead-letter entry");
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

  private SourceCommand runCommand(
      EncodingType encodingType,
      int maxRetries,
      CapturingDlqWriter dlq,
      CollectingAcceptor acceptor)
      throws Exception {
    return runCommand(encodingType, maxRetries, 30, dlq, acceptor);
  }

  private SourceCommand runCommand(
      EncodingType encodingType,
      int maxRetries,
      int visibilityTimeout,
      CapturingDlqWriter dlq,
      CollectingAcceptor acceptor)
      throws Exception {
    Queue<String> confirmed = new ConcurrentLinkedQueue<>();
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer();
    // Command-owned clients: terminate() closes these via the fetcher, leaving the test's own
    // control-plane clients (s3/sqs) untouched for setup and assertions.
    S3Client commandS3 = newS3Client();
    SqsClient commandSqs = newSqsClient();
    RawDataConverter<S3EventMessage> converter =
        new S3RealtimeRawDataConverter(
            commandS3, deserializer, null, 256L * 1024 * 1024, false, confirmed);
    Fetcher<S3EventMessage> fetcher =
        new S3RealtimeSourceFetcher(
            commandSqs,
            commandS3,
            queueUrl,
            10,
            1,
            visibilityTimeout,
            maxRetries,
            dlq,
            "node",
            confirmed);
    SourceExecutionContext<S3EventMessage> ctx =
        new SourceExecutionContext<>(
            fetcher,
            converter,
            new S3RealtimeRawDataEncoder(),
            mock(FleakCounter.class),
            mock(FleakCounter.class),
            mock(FleakCounter.class),
            dlq);

    TestS3RealtimeSourceCommand command = new TestS3RealtimeSourceCommand(ctx);
    command.initialize(mock(MetricClientProvider.class));
    executor.submit(
        () -> {
          try {
            command.execute("user", acceptor);
          } catch (Exception e) {
            log.error("command execution failed", e);
          }
        });
    return command;
  }

  private void sendNotification(String bucket, String key) {
    String body =
        "{\"Records\":[{\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"bucket\":{\"name\":\""
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

  private static class TestS3RealtimeSourceCommand extends SimpleSourceCommand<S3EventMessage> {
    private final SourceExecutionContext<S3EventMessage> ctx;

    TestS3RealtimeSourceCommand(SourceExecutionContext<S3EventMessage> ctx) {
      super("node", io.fleak.zephflow.lib.TestUtils.buildJobContext(new HashMap<>()), null, null);
      this.ctx = ctx;
    }

    @Override
    protected ExecutionContext createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      return ctx;
    }

    @Override
    public String commandName() {
      return "s3rtsource";
    }

    @Override
    public SourceType sourceType() {
      return SourceType.STREAMING;
    }
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
