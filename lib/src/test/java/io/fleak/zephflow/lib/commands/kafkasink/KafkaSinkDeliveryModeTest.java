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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * FLE-2366: kafkasink's default delivery mode must wait for broker acks before {@code flush()}
 * returns. The source checkpoint advances as soon as the sink returns, so a flusher that returns
 * while records are still in the producer's client-side accumulator turns a crash (SIGKILL) into
 * silent, unrecoverable data loss. Fire-and-forget is throughput-over-durability and must be an
 * explicit opt-in, decoupled from store-and-forward.
 */
class KafkaSinkDeliveryModeTest {

  private static final String TOPIC = "delivery_mode_topic";

  private static final List<RecordFleakData> EVENTS =
      List.of(
          (RecordFleakData) FleakData.wrap(Map.of("num", 0)),
          (RecordFleakData) FleakData.wrap(Map.of("num", 1)),
          (RecordFleakData) FleakData.wrap(Map.of("num", 2)));

  private KafkaProducer<byte[], byte[]> mockProducer;
  private Properties capturedProducerProps;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockProducer = mock(KafkaProducer.class);
    when(mockProducer.partitionsFor(TOPIC))
        .thenReturn(List.of(new PartitionInfo(TOPIC, 0, null, null, null)));
    // Synchronous path: send(record) returns an already-acked future.
    when(mockProducer.send(any(ProducerRecord.class)))
        .thenAnswer(inv -> CompletableFuture.completedFuture(mock(RecordMetadata.class)));
    // Fire-and-forget path: send(record, callback) reports success via the callback.
    when(mockProducer.send(any(ProducerRecord.class), any(Callback.class)))
        .thenAnswer(
            inv -> {
              Callback callback = inv.getArgument(1);
              callback.onCompletion(mock(RecordMetadata.class), null);
              return CompletableFuture.completedFuture(mock(RecordMetadata.class));
            });
  }

  private KafkaSinkCommand buildCommand(KafkaSinkDto.Config config) {
    KafkaProducerClientFactory producerFactory =
        new KafkaProducerClientFactory() {
          @Override
          KafkaProducer<byte[], byte[]> createKafkaProducer(Properties props) {
            capturedProducerProps = props;
            return mockProducer;
          }
        };
    KafkaSinkCommand command =
        (KafkaSinkCommand)
            new KafkaSinkCommandFactory(producerFactory)
                .createCommand("delivery_mode_node", TestUtils.JOB_CONTEXT);
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    return command;
  }

  private static KafkaSinkDto.Config.ConfigBuilder baseConfig() {
    return KafkaSinkDto.Config.builder()
        .topic(TOPIC)
        .broker("localhost:9092")
        .encodingType(EncodingType.JSON_OBJECT.toString());
  }

  @Test
  void defaultDeliveryMode_waitsForBrokerAcks() {
    KafkaSinkCommand command = buildCommand(baseConfig().build());

    ScalarSinkCommand.SinkResult result =
        command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    assertEquals(EVENTS.size(), result.getSuccessCount());
    // Synchronous shape: every record sent without a callback, then one producer.flush(), then the
    // per-record futures are awaited. Fire-and-forget would use send(record, callback) and never
    // call flush().
    InOrder inOrder = inOrder(mockProducer);
    inOrder.verify(mockProducer, times(EVENTS.size())).send(any(ProducerRecord.class));
    inOrder.verify(mockProducer).flush();
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  void defaultDeliveryMode_producerIsDurable() {
    buildCommand(baseConfig().build());

    assertEquals("all", capturedProducerProps.get(ProducerConfig.ACKS_CONFIG));
    assertEquals("true", capturedProducerProps.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
  }

  @Test
  void fireAndForgetOptIn_usesAsyncSendPath() {
    KafkaSinkCommand command =
        buildCommand(baseConfig().deliveryMode(KafkaSinkDto.DeliveryMode.FIRE_AND_FORGET).build());

    ScalarSinkCommand.SinkResult result =
        command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    assertEquals(EVENTS.size(), result.getSuccessCount());
    verify(mockProducer, times(EVENTS.size())).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockProducer, never()).send(any(ProducerRecord.class));
    verify(mockProducer, never()).flush();
  }

  @Test
  void fireAndForgetOptIn_keepsThroughputProducerProps() {
    buildCommand(baseConfig().deliveryMode(KafkaSinkDto.DeliveryMode.FIRE_AND_FORGET).build());

    assertEquals("1", capturedProducerProps.get(ProducerConfig.ACKS_CONFIG));
    assertNull(capturedProducerProps.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
  }

  @Test
  void userAcksOverride_suppressesIdempotence() {
    // A user explicitly weakening acks must not collide with enable.idempotence=true, which the
    // producer rejects at construction time unless acks=all.
    buildCommand(baseConfig().properties(Map.of(ProducerConfig.ACKS_CONFIG, "1")).build());

    assertEquals("1", capturedProducerProps.get(ProducerConfig.ACKS_CONFIG));
    assertNull(capturedProducerProps.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
  }

  @Test
  void userRetriesZeroOverride_suppressesIdempotence() {
    // enable.idempotence=true also requires retries > 0; a pre-existing config that pinned
    // retries=0 must keep constructing a valid producer.
    buildCommand(baseConfig().properties(Map.of(ProducerConfig.RETRIES_CONFIG, "0")).build());

    assertEquals("0", capturedProducerProps.get(ProducerConfig.RETRIES_CONFIG));
    assertNull(capturedProducerProps.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
  }

  @Test
  void userMaxInFlightOverride_suppressesIdempotence() {
    // enable.idempotence=true requires max.in.flight <= 5; same backward-compat concern.
    buildCommand(
        baseConfig()
            .properties(Map.of(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6"))
            .build());

    assertNull(capturedProducerProps.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
  }

  @Test
  void waitForAck_failedAckBecomesErrorOutput_notSuccess() {
    // The heart of FLE-2366: a record whose broker ack fails must never be reported as delivered,
    // because the source checkpoint advances on the sink's word.
    java.util.concurrent.atomic.AtomicInteger sendCount =
        new java.util.concurrent.atomic.AtomicInteger();
    when(mockProducer.send(any(ProducerRecord.class)))
        .thenAnswer(
            inv ->
                sendCount.incrementAndGet() == 2
                    ? CompletableFuture.failedFuture(new RuntimeException("ack failed"))
                    : CompletableFuture.completedFuture(mock(RecordMetadata.class)));

    KafkaSinkCommand command = buildCommand(baseConfig().build());
    ScalarSinkCommand.SinkResult result =
        command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    assertEquals(EVENTS.size() - 1, result.getSuccessCount());
    assertEquals(1, result.errorCount());
  }

  @Test
  void waitForAck_brokerUnreachable_failsBatchBeforeAnySend() {
    // During an outage the pre-send metadata probe must fail the whole batch after ONE bounded
    // wait instead of silently proceeding into per-record max.block.ms waits.
    when(mockProducer.partitionsFor(TOPIC))
        .thenThrow(new org.apache.kafka.common.errors.TimeoutException("no metadata"));

    KafkaSinkCommand command = buildCommand(baseConfig().build());
    ScalarSinkCommand.SinkResult result =
        command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    assertEquals(0, result.getSuccessCount());
    assertEquals(EVENTS.size(), result.errorCount());
    verify(mockProducer, never()).send(any(ProducerRecord.class));
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  void legacyConfigWithoutDeliveryModeField_getsDurableDefault() {
    // Configs written before this field existed arrive as raw maps with no deliveryMode key and
    // are deserialized through the no-args constructor, which skips @Builder.Default. They must
    // still get the ack-waiting default.
    KafkaProducerClientFactory producerFactory =
        new KafkaProducerClientFactory() {
          @Override
          KafkaProducer<byte[], byte[]> createKafkaProducer(Properties props) {
            capturedProducerProps = props;
            return mockProducer;
          }
        };
    KafkaSinkCommand command =
        (KafkaSinkCommand)
            new KafkaSinkCommandFactory(producerFactory)
                .createCommand("legacy_config_node", TestUtils.JOB_CONTEXT);
    command.parseAndValidateArg(
        Map.of("broker", "localhost:9092", "topic", TOPIC, "encodingType", "JSON_OBJECT"));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());

    command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    assertEquals("all", capturedProducerProps.get(ProducerConfig.ACKS_CONFIG));
    verify(mockProducer).flush();
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  void storeAndForward_impliesWaitForAck() {
    // storeAndForwardEnabled has always meant synchronous delivery; the new default must not
    // change that, and the two knobs must agree.
    KafkaSinkDto.Config config = baseConfig().storeAndForwardEnabled(true).build();
    assertEquals(KafkaSinkDto.DeliveryMode.WAIT_FOR_ACK, config.getDeliveryMode());
  }
}
