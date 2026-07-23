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
package io.fleak.zephflow.lib.commands.kafkasink;

import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_ENV;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_SERVICE;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.commands.sink.SinkStoreForward;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Blackbox integration test for store-and-forward on the Kafka sink against a <b>real broker</b>.
 * The outage is a real {@code docker pause} of the broker container, and recovery is {@code
 * unpause}. We verify: nothing is delivered while the broker is down, records accumulate in the
 * local on-disk store, and after recovery every record arrives in the topic (no loss).
 */
@Tag("integration")
@Testcontainers
class KafkaSinkStoreForwardIntegrationTest {

  private static final String TOPIC = "store_forward_topic";
  private static final int HEALTHY = 50; // ids 1..50, delivered before the outage
  private static final int OUTAGE = 100; // ids 51..150, buffered during the outage
  private static final int TOTAL = HEALTHY + OUTAGE;

  @Container
  private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

  @BeforeAll
  static void setup() throws Exception {
    createTopic(TOPIC);
  }

  /** Single partition so a consumer reading by offset sees one totally-ordered stream. */
  private static void createTopic(String name) throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    try (AdminClient admin = AdminClient.create(props)) {
      admin
          .createTopics(Collections.singleton(new NewTopic(name, 1, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
    }
  }

  @AfterAll
  static void stop() {
    if (KAFKA.isRunning()) {
      KAFKA.stop();
    }
  }

  @Test
  void bufferLocallyDuringBrokerOutageThenDeliverEverythingOnRecovery(@TempDir Path storeDir)
      throws Exception {
    CapturingMetrics metrics = new CapturingMetrics();
    String nodeId = "kafka-sf-node";

    KafkaSinkCommand sink =
        (KafkaSinkCommand) new KafkaSinkCommandFactory().createCommand(nodeId, jobContext());
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .topic(TOPIC)
            .broker(KAFKA.getBootstrapServers())
            .encodingType(EncodingType.JSON_OBJECT.toString())
            .storeAndForwardEnabled(true)
            .localStorePath(storeDir.toString())
            .forwardRetryIntervalMillis(1_000)
            .build();
    sink.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    sink.initialize(metrics);

    var ctx = (SinkExecutionContext<RecordFleakData>) sink.getExecutionContext();
    SinkStoreForward storeForward = ctx.storeForward();

    // 1) Healthy: deliver ids 1..HEALTHY straight to the broker.
    sink.writeToSink(records(1, HEALTHY), "user", ctx);
    assertFalse(storeForward.isBuffering());
    assertEquals(HEALTHY, new TreeSet<>(consumeIds(Duration.ofSeconds(10))).size());

    // 2) Cut the network: pause the broker container while the producer is idle.
    pauseBroker();
    try {
      // first outage write trips into BUFFERING (a bounded sync flush fails fast);
      sink.writeToSink(records(HEALTHY + 1, HEALTHY + 60), "user", ctx);
      // subsequent writes take the outage fast-path straight to disk.
      sink.writeToSink(records(HEALTHY + 61, TOTAL), "user", ctx);

      assertTrue(storeForward.isBuffering(), "sink should be buffering during the outage");
      assertEquals(OUTAGE, metrics.total(METRIC_BUFFERED), "all outage records written locally");
      assertTrue(storeDirBytes(storeDir) > 0, "local store should hold data on disk");
    } finally {
      // 3) Restore the network.
      unpauseBroker();
    }

    // 4) Recovery: the forwarder drains the backlog back to the broker.
    await(() -> !storeForward.isBuffering(), 90);
    assertEquals(OUTAGE, metrics.total(METRIC_REPLAYED), "all buffered records replayed");
    assertEquals(0, metrics.total(METRIC_DROPPED), "nothing dropped");

    // Draining reclaims the disk: the queue files are deleted, leaving only empty lock/marker
    // files.
    await(() -> quietStoreDirBytes(storeDir) == 0L, 30);

    // 5) Every record (healthy + replayed) is in the topic exactly once, no loss.
    TreeSet<Integer> all = new TreeSet<>(consumeAllIds());
    assertEquals(TOTAL, all.size());
    assertEquals(1, all.first());
    assertEquals(TOTAL, all.last());

    sink.terminate();
  }

  @Test
  void streamContinuouslyThroughRecoveryPreservesEveryRecordInOrder(@TempDir Path storeDir)
      throws Exception {
    String topic = "stream_through_recovery_topic";
    createTopic(topic);
    CapturingMetrics metrics = new CapturingMetrics();
    // Idempotent producer so Kafka's own retries can't reorder independently of store-and-forward;
    // that isolates the ordering assertion to OUR buffer->drain->direct handoff.
    KafkaSinkCommand sink =
        initSink(
            "kafka-sf-stream",
            topic,
            storeDir,
            metrics,
            Map.of("enable.idempotence", "true", "acks", "all"));
    var ctx = (SinkExecutionContext<RecordFleakData>) sink.getExecutionContext();
    SinkStoreForward storeForward = ctx.storeForward();

    // A producer that never stops: it writes monotonically increasing ids the whole time, including
    // while the broker is down and while the backlog is draining.
    AtomicInteger nextId = new AtomicInteger(1);
    AtomicInteger lastIssued = new AtomicInteger(0);
    AtomicReference<Throwable> error = new AtomicReference<>();
    AtomicBoolean running = new AtomicBoolean(true);
    Thread producer =
        new Thread(
            () -> {
              while (running.get()) {
                int id = nextId.getAndIncrement();
                try {
                  sink.writeToSink(records(id, id), "user", ctx);
                  lastIssued.set(id);
                  Thread.sleep(2);
                } catch (InterruptedException e) {
                  return;
                } catch (Throwable t) {
                  error.set(t);
                  return;
                }
              }
            },
            "test-producer");

    boolean paused = false;
    try {
      producer.start();
      Thread.sleep(1_500); // stream healthy
      pauseBroker();
      paused = true;
      Thread.sleep(7_000); // keep streaming through the outage (first outage write blocks ~5s)
      unpauseBroker();
      paused = false;
      Thread.sleep(3_000); // keep streaming while the backlog drains
    } finally {
      running.set(false);
      producer.join(TimeUnit.SECONDS.toMillis(15));
      if (paused) {
        try {
          unpauseBroker();
        } catch (RuntimeException ignore) {
          // already running
        }
      }
    }

    assertNull(error.get(), "producer thread should not throw");
    await(() -> !storeForward.isBuffering(), 90);

    int totalSent = lastIssued.get();
    assertTrue(totalSent > 50, "expected a meaningful stream, got " + totalSent);

    List<Integer> consumed =
        drain("stream-final-" + System.nanoTime(), topic, Duration.ofSeconds(30), totalSent);

    // No loss: every id 1..totalSent reached the topic.
    TreeSet<Integer> distinct = new TreeSet<>(consumed);
    assertEquals(totalSent, distinct.size(), "no loss");
    assertEquals(1, distinct.first());
    assertEquals(totalSent, distinct.last());
    // No reordering across the buffer->drain->direct handoff (single partition = offset order).
    // Duplicates from at-least-once are tolerated, so the check is non-decreasing, not strictly so.
    for (int i = 1; i < consumed.size(); i++) {
      assertTrue(
          consumed.get(i) >= consumed.get(i - 1),
          "out of order at index " + i + ": " + consumed.get(i - 1) + " -> " + consumed.get(i));
    }
    assertEquals(0, metrics.total(METRIC_DROPPED), "nothing dropped");

    sink.terminate();
  }

  @Test
  void bufferFromColdStartWhenBrokerDownAtStartup(@TempDir Path storeDir) throws Exception {
    String topic = "startup_down_topic";
    createTopic(topic); // broker is up here
    int n = 80;
    CapturingMetrics metrics = new CapturingMetrics();

    // Bring the broker down BEFORE the sink is initialized: the first write must take the
    // cold-metadata path (no cached metadata) and buffer the whole batch.
    pauseBroker();
    KafkaSinkCommand sink = initSink("kafka-sf-coldstart", topic, storeDir, metrics, null);
    var ctx = (SinkExecutionContext<RecordFleakData>) sink.getExecutionContext();
    SinkStoreForward storeForward = ctx.storeForward();
    try {
      sink.writeToSink(records(1, n), "user", ctx);
      assertTrue(storeForward.isBuffering(), "should buffer when the broker is down at startup");
      assertEquals(n, metrics.total(METRIC_BUFFERED), "all records written locally");
      assertTrue(storeDirBytes(storeDir) > 0, "local store should hold data on disk");
    } finally {
      unpauseBroker();
    }

    await(() -> !storeForward.isBuffering(), 90);
    assertEquals(n, metrics.total(METRIC_REPLAYED), "all buffered records replayed");
    assertEquals(0, metrics.total(METRIC_DROPPED), "nothing dropped");

    TreeSet<Integer> all =
        new TreeSet<>(
            drain("coldstart-final-" + System.nanoTime(), topic, Duration.ofSeconds(20), n));
    assertEquals(n, all.size());
    assertEquals(1, all.first());
    assertEquals(n, all.last());

    sink.terminate();
  }

  // ===== helpers =====

  private KafkaSinkCommand initSink(
      String nodeId,
      String topic,
      Path storeDir,
      MetricClientProvider metrics,
      Map<String, String> extraProps) {
    KafkaSinkCommand sink =
        (KafkaSinkCommand) new KafkaSinkCommandFactory().createCommand(nodeId, jobContext());
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .topic(topic)
            .broker(KAFKA.getBootstrapServers())
            .encodingType(EncodingType.JSON_OBJECT.toString())
            .storeAndForwardEnabled(true)
            .localStorePath(storeDir.toString())
            .forwardRetryIntervalMillis(1_000)
            .properties(extraProps)
            .build();
    sink.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    sink.initialize(metrics);
    return sink;
  }

  private static List<RecordFleakData> records(int fromInclusive, int toInclusive) {
    List<RecordFleakData> out = new ArrayList<>();
    for (int id = fromInclusive; id <= toInclusive; id++) {
      out.add((RecordFleakData) FleakData.wrap(Map.of("id", id)));
    }
    return out;
  }

  /** Consumes from the current group offset and returns the ids seen. */
  private List<Integer> consumeIds(Duration timeout) {
    return drain("running-consumer", TOPIC, timeout, HEALTHY);
  }

  /** Fresh consumer from the beginning of the topic; returns all ids currently present. */
  private List<Integer> consumeAllIds() {
    return drain("final-consumer-" + System.nanoTime(), TOPIC, Duration.ofSeconds(20), TOTAL);
  }

  /** Reads up to {@code expected} ids from {@code topic} in offset (delivery) order. */
  private List<Integer> drain(String group, String topic, Duration overall, int expected) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    List<Integer> ids = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      long deadline = System.nanoTime() + overall.toNanos();
      // Stop on distinct count, not raw size, so at-least-once duplicates don't hide a tail.
      while (System.nanoTime() < deadline && new HashSet<>(ids).size() < expected) {
        ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
        StreamSupport.stream(polled.spliterator(), false)
            .map(
                r -> fromJsonString(new String(r.value()), new TypeReference<RecordFleakData>() {}))
            .map(rec -> ((Number) rec.unwrap().get("id")).intValue())
            .forEach(ids::add);
      }
    }
    return ids;
  }

  private static long quietStoreDirBytes(Path dir) {
    try {
      return storeDirBytes(dir);
    } catch (Exception e) {
      return -1;
    }
  }

  private static long storeDirBytes(Path dir) throws Exception {
    try (var paths = Files.walk(dir)) {
      return paths
          .filter(Files::isRegularFile)
          .mapToLong(KafkaSinkStoreForwardIntegrationTest::size)
          .sum();
    }
  }

  private static long size(Path p) {
    try {
      return Files.size(p);
    } catch (Exception e) {
      return 0;
    }
  }

  private static void pauseBroker() {
    DockerClientFactory.instance().client().pauseContainerCmd(KAFKA.getContainerId()).exec();
  }

  private static void unpauseBroker() {
    DockerClientFactory.instance().client().unpauseContainerCmd(KAFKA.getContainerId()).exec();
  }

  private static JobContext jobContext() {
    return JobContext.builder()
        .metricTags(Map.of(METRIC_TAG_SERVICE, "test_service", METRIC_TAG_ENV, "test_env"))
        .otherProperties(Map.of())
        .build();
  }

  private static void await(BooleanSupplier condition, int timeoutSeconds) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(200);
    }
    fail("condition not met within " + timeoutSeconds + "s");
  }

  private static final String METRIC_BUFFERED = "store_forward_buffered_count";
  private static final String METRIC_REPLAYED = "store_forward_replayed_count";
  private static final String METRIC_DROPPED = "store_forward_dropped_count";

  /** Captures counter totals by metric name; gauges/stopwatches fall back to the no-op base. */
  private static final class CapturingMetrics
      extends MetricClientProvider.NoopMetricClientProvider {
    private final Map<String, AtomicLong> totals = new ConcurrentHashMap<>();

    long total(String name) {
      return totals.getOrDefault(name, new AtomicLong()).get();
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
