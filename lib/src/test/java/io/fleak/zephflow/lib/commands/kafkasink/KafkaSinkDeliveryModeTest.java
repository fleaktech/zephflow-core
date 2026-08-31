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
  void storeAndForward_impliesWaitForAck() {
    // storeAndForwardEnabled has always meant synchronous delivery; the new default must not
    // change that, and the two knobs must agree.
    KafkaSinkDto.Config config = baseConfig().storeAndForwardEnabled(true).build();
    assertEquals(KafkaSinkDto.DeliveryMode.WAIT_FOR_ACK, config.getDeliveryMode());
  }
}
