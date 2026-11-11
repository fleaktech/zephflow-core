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

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.encoding;
import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.commands.sink.SinkStoreForward;
import io.fleak.zephflow.lib.dlq.S3DlqWriterTest;
import io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

@Testcontainers
class S3SinkStoreForwardTest {

  private static final String REGION = "us-east-1";
  private static final String NODE_ID = "s3-sf-node";
  private static final String TEXT_LINE = "{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}";
  private static final String BUFFERED = "store_forward_buffered_count";
  private static final String REPLAYED = "store_forward_replayed_count";
  private static final String DROPPED = "store_forward_dropped_count";
  private static final String SINK_OUTPUT = "sink_output_count";
  private static final String SINK_ERROR = "sink_error_count";
  private static final List<String> COUNTERS =
      List.of(BUFFERED, REPLAYED, DROPPED, SINK_OUTPUT, SINK_ERROR);

  @Container
  static MinIOContainer minio =
      new MinIOContainer(TestUtils.MINIO_IMAGE).withCommand("server /data");

  private S3Client s3Client;
  private String bucket;
  private final List<S3SinkCommand> sinks = new ArrayList<>();

  @BeforeEach
  void setUp() {
    System.setProperty("aws.accessKeyId", minio.getUserName());
    System.setProperty("aws.secretAccessKey", minio.getPassword());
    s3Client = new AwsClientFactory().createS3Client(REGION, null, minio.getS3URL());
    bucket = "sf-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
  }

  @AfterEach
  void tearDown() throws IOException {
    for (S3SinkCommand sink : sinks) {
      sink.terminate();
    }
    try {
      S3DlqWriterTest.deleteAllObjectsInBucket(s3Client, bucket);
      s3Client.deleteBucket(b -> b.bucket(bucket));
    } catch (S3Exception ignored) {
    }
    s3Client.close();
  }

  @Test
  void timerFlushToAMissingBucketIsStoredAndUploadedOnceTheBucketExists(@TempDir Path storeDir)
      throws Exception {
    CapturingMetrics metrics = new CapturingMetrics();
    S3SinkCommand sink = start(jsonConfig(storeDir, true), metrics);
    SinkStoreForward store = store(sink);

    sink.writeToSink(records(0, 100), "user", sink.getExecutionContext());
    await(() -> metrics.total(BUFFERED) == 100);
    boolean bufferingDuringOutage = store.isBuffering();
    createBucket();
    await(() -> !store.isBuffering());

    assertEquals(
        List.of(
            true,
            counts(100, 100, 0, 100, 0),
            IntStream.range(0, 100).boxed().collect(Collectors.toCollection(TreeSet::new))),
        List.of(bufferingDuringOutage, metrics.totals(), uploadedIds()));
  }

  @Test
  void aTextRecordWithoutRawIsASinkErrorAndTheOthersAreStoredAndUploaded(@TempDir Path storeDir)
      throws Exception {
    CapturingMetrics metrics = new CapturingMetrics();
    S3SinkCommand sink = start(textConfig(storeDir), metrics);
    SinkStoreForward store = store(sink);
    List<String> lines = IntStream.range(0, 9).mapToObj(i -> "line " + i).toList();
    List<RecordFleakData> batch =
        new ArrayList<>(
            lines.stream()
                .map(line -> (RecordFleakData) FleakData.wrap(Map.of("__raw__", line)))
                .toList());
    batch.add(4, (RecordFleakData) FleakData.wrap(Map.of("message", "no raw")));

    sink.writeToSink(batch, "user", sink.getExecutionContext());
    await(() -> metrics.total(BUFFERED) == 9 && metrics.total(SINK_ERROR) == 1);
    createBucket();
    await(() -> !store.isBuffering());

    assertEquals(
        List.of(counts(9, 9, 0, 9, 1), List.of(String.join("\n", lines) + "\n")),
        List.of(metrics.totals(), uploadedObjects()));
  }

  @Test
  void storeOffCountsTheLossAndWritesNothingLocally(@TempDir Path storeDir) throws Exception {
    CapturingMetrics metrics = new CapturingMetrics();
    S3SinkCommand sink = start(jsonConfig(storeDir, false), metrics);

    sink.writeToSink(records(0, 3), "user", sink.getExecutionContext());
    await(() -> metrics.total(SINK_ERROR) == 3);

    assertEquals(
        List.of(SinkStoreForward.noop(), counts(0, 0, 0, 0, 3), List.of()),
        List.of(store(sink), metrics.totals(), filesUnder(storeDir)));
  }

  @Test
  void storedRecordsAreUploadedByASinkReopenedOnTheSameStorePath(@TempDir Path storeDir)
      throws Exception {
    CapturingMetrics firstRun = new CapturingMetrics();
    S3SinkCommand first = start(jsonConfig(storeDir, true), firstRun);
    first.writeToSink(records(0, 50), "user", first.getExecutionContext());
    await(() -> firstRun.total(BUFFERED) == 50);
    sinks.remove(first);
    first.terminate();
    createBucket();

    CapturingMetrics secondRun = new CapturingMetrics();
    S3SinkCommand second = start(jsonConfig(storeDir, true), secondRun);
    SinkStoreForward store = store(second);
    await(() -> secondRun.total(REPLAYED) == 50 && !store.isBuffering());

    assertEquals(
        List.of(
            counts(0, 50, 0, 50, 0),
            IntStream.range(0, 50).boxed().collect(Collectors.toCollection(TreeSet::new))),
        List.of(secondRun.totals(), uploadedIds()));
  }

  @Test
  void testModeKeepsStoreAndForwardOff(@TempDir Path storeDir) throws Exception {
    HashMap<String, Serializable> otherProperties = new HashMap<>();
    otherProperties.put(JobContext.FLAG_TEST_MODE, true);

    S3SinkCommand sink = start(jsonConfig(storeDir, true), new CapturingMetrics(), otherProperties);

    assertEquals(
        List.of(SinkStoreForward.noop(), List.of()), List.of(store(sink), filesUnder(storeDir)));
  }

  @Test
  void storeFieldsDefaultWhenAbsentFromJson() throws Exception {
    S3SinkDto.Config parsed =
        OBJECT_MAPPER.readValue(
            "{\"regionStr\":\"us-east-1\",\"bucketName\":\"b\",\"keyName\":\"k\"}",
            S3SinkDto.Config.class);

    assertEquals(
        S3SinkDto.Config.builder()
            .regionStr("us-east-1")
            .bucketName("b")
            .keyName("k")
            .storeAndForwardEnabled(false)
            .localStoreMaxBytes(1_073_741_824L)
            .forwardRetryIntervalMillis(5_000L)
            .build(),
        parsed);
  }

  private S3SinkDto.Config.ConfigBuilder baseConfig(Path storeDir, boolean storeAndForward) {
    return S3SinkDto.Config.builder()
        .regionStr(REGION)
        .bucketName(bucket)
        .keyName("sf")
        .s3EndpointOverride(minio.getS3URL())
        .batchSize(1000)
        .flushIntervalMillis(300)
        .storeAndForwardEnabled(storeAndForward)
        .localStorePath(storeDir.toString())
        .forwardRetryIntervalMillis(200);
  }

  private S3SinkDto.Config jsonConfig(Path storeDir, boolean storeAndForward) {
    return baseConfig(storeDir, storeAndForward)
        .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"lines\"}"))
        .build();
  }

  private S3SinkDto.Config textConfig(Path storeDir) {
    return baseConfig(storeDir, true).encoding(encoding("[]", TEXT_LINE)).build();
  }

  private S3SinkCommand start(S3SinkDto.Config config, MetricClientProvider metrics) {
    return start(config, metrics, new HashMap<>());
  }

  private S3SinkCommand start(
      S3SinkDto.Config config,
      MetricClientProvider metrics,
      HashMap<String, Serializable> otherProperties) {
    JobContext jobContext =
        JobContext.builder()
            .metricTags(JOB_CONTEXT.getMetricTags())
            .otherProperties(otherProperties)
            .build();
    S3SinkCommand sink =
        (S3SinkCommand) new S3SinkCommandFactory().createCommand(NODE_ID, jobContext);
    sink.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    sink.initialize(metrics);
    sinks.add(sink);
    return sink;
  }

  @SuppressWarnings("unchecked")
  private static SinkStoreForward store(S3SinkCommand sink) {
    return ((SinkExecutionContext<RecordFleakData>) sink.getExecutionContext()).storeForward();
  }

  private static List<RecordFleakData> records(int from, int to) {
    return IntStream.range(from, to)
        .mapToObj(i -> (RecordFleakData) FleakData.wrap(Map.of("id", i)))
        .toList();
  }

  private static Map<String, Long> counts(
      long buffered, long replayed, long dropped, long output, long errors) {
    return Map.of(
        BUFFERED,
        buffered,
        REPLAYED,
        replayed,
        DROPPED,
        dropped,
        SINK_OUTPUT,
        output,
        SINK_ERROR,
        errors);
  }

  private void createBucket() {
    s3Client.createBucket(b -> b.bucket(bucket));
  }

  private List<String> uploadedObjects() {
    return s3Client.listObjectsV2(b -> b.bucket(bucket)).contents().stream()
        .map(S3Object::key)
        .map(
            key ->
                new String(
                    s3Client.getObjectAsBytes(b -> b.bucket(bucket).key(key)).asByteArray(), UTF_8))
        .toList();
  }

  private TreeSet<Integer> uploadedIds() {
    return uploadedObjects().stream()
        .flatMap(content -> Arrays.stream(content.split("\n")))
        .filter(line -> !line.isBlank())
        .map(S3SinkStoreForwardTest::id)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  private static int id(String jsonLine) {
    try {
      return OBJECT_MAPPER.readTree(jsonLine).get("id").asInt();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static List<Path> filesUnder(Path dir) throws IOException {
    try (Stream<Path> paths = Files.walk(dir)) {
      return paths.filter(Files::isRegularFile).toList();
    }
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(100);
    }
    fail("condition not met within 60s");
  }

  private static final class CapturingMetrics
      extends MetricClientProvider.NoopMetricClientProvider {
    private final Map<String, AtomicLong> totals = new ConcurrentHashMap<>();

    long total(String name) {
      return totals.getOrDefault(name, new AtomicLong()).get();
    }

    Map<String, Long> totals() {
      return COUNTERS.stream().collect(Collectors.toMap(name -> name, this::total));
    }

    @Override
    public FleakCounter counter(String name, Map<String, String> tags) {
      AtomicLong total = totals.computeIfAbsent(name, k -> new AtomicLong());
      return new FleakCounter() {
        @Override
        public void increase(Map<String, String> additionalTags) {
          total.incrementAndGet();
        }

        @Override
        public void increase(long n, Map<String, String> additionalTags) {
          total.addAndGet(n);
        }
      };
    }
  }
}
