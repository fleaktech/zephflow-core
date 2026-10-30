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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/** Delivery-mode selection and producer configuration for {@link KafkaSinkCommand} (FLE-2366). */
class KafkaSinkDeliveryModeTest {

  private static final String TOPIC = "delivery_mode_topic";

  private static final List<RecordFleakData> EVENTS =
      List.of(
          (RecordFleakData) FleakData.wrap(Map.of("num", 0)),
          (RecordFleakData) FleakData.wrap(Map.of("num", 1)),
          (RecordFleakData) FleakData.wrap(Map.of("num", 2)));

  private KafkaProducer<byte[], byte[]> mockProducer;
  private Properties capturedProducerProperties;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockProducer = mock(KafkaProducer.class);
    when(mockProducer.partitionsFor(TOPIC))
        .thenReturn(List.of(new PartitionInfo(TOPIC, 0, null, null, null)));
    when(mockProducer.send(any(ProducerRecord.class)))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(mock(RecordMetadata.class)));
    when(mockProducer.send(any(ProducerRecord.class), any(Callback.class)))
        .thenAnswer(
            invocation -> {
              Callback callback = invocation.getArgument(1);
              callback.onCompletion(mock(RecordMetadata.class), null);
              return CompletableFuture.completedFuture(mock(RecordMetadata.class));
            });
  }

  private KafkaSinkCommand buildCommand(KafkaSinkDto.Config config) {
    Map<String, Object> configMap = OBJECT_MAPPER.convertValue(config, new TypeReference<>() {});
    return buildCommandFromConfigMap(configMap);
  }

  private KafkaSinkCommand buildDeferredCommand(KafkaSinkDto.Config config) {
    Map<String, Object> configMap = OBJECT_MAPPER.convertValue(config, new TypeReference<>() {});
    return buildCommandFromConfigMap(configMap, true);
  }

  private KafkaSinkCommand buildCommandFromConfigMap(Map<String, Object> configMap) {
    return buildCommandFromConfigMap(configMap, false);
  }

  private KafkaSinkCommand buildCommandFromConfigMap(
      Map<String, Object> configMap, boolean deferDeliveryConfirmation) {
    KafkaProducerClientFactory producerClientFactory =
        new KafkaProducerClientFactory() {
          @Override
          KafkaProducer<byte[], byte[]> createKafkaProducer(Properties producerProperties) {
            capturedProducerProperties = producerProperties;
            return mockProducer;
          }
        };
    KafkaSinkCommand command =
        (KafkaSinkCommand)
            new KafkaSinkCommandFactory(producerClientFactory)
                .createCommand("delivery_mode_node", TestUtils.JOB_CONTEXT);
    command.parseAndValidateArg(configMap);
    if (deferDeliveryConfirmation) {
      command.deferDeliveryConfirmation();
    }
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
  void defaultDeliveryMode_sendsBatchThenFlushesAndWaitsForBrokerAcks() {
    KafkaSinkCommand command = buildCommand(baseConfig().build());

    ScalarSinkCommand.SinkResult result =
        command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    assertEquals(EVENTS.size(), result.getSuccessCount());
    InOrder inOrder = inOrder(mockProducer);
    inOrder.verify(mockProducer, times(EVENTS.size())).send(any(ProducerRecord.class));
    inOrder.verify(mockProducer).flush();
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  void defaultDeliveryMode_enablesAcksAllAndIdempotence() {
    buildCommand(baseConfig().build());

    assertEquals("all", capturedProducerProperties.get(ProducerConfig.ACKS_CONFIG));
    assertEquals("true", capturedProducerProperties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
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
  void fireAndForgetOptIn_keepsAcksOneWithoutIdempotence() {
    buildCommand(baseConfig().deliveryMode(KafkaSinkDto.DeliveryMode.FIRE_AND_FORGET).build());

    assertEquals("1", capturedProducerProperties.get(ProducerConfig.ACKS_CONFIG));
    assertNull(capturedProducerProperties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
  }

  @Test
  void userAcksOverride_suppressesIdempotence() {
    buildCommand(baseConfig().properties(Map.of(ProducerConfig.ACKS_CONFIG, "1")).build());

    assertEquals("1", capturedProducerProperties.get(ProducerConfig.ACKS_CONFIG));
    assertNull(capturedProducerProperties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
  }

  @Test
  void userRetriesZeroOverride_suppressesIdempotence() {
    buildCommand(baseConfig().properties(Map.of(ProducerConfig.RETRIES_CONFIG, "0")).build());

    assertEquals("0", capturedProducerProperties.get(ProducerConfig.RETRIES_CONFIG));
    assertNull(capturedProducerProperties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
  }

  @Test
  void userMaxInFlightOverride_suppressesIdempotence() {
    buildCommand(
        baseConfig()
            .properties(Map.of(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6"))
            .build());

    assertNull(capturedProducerProperties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
  }

  @Test
  void defaultDeliveryMode_failedBrokerAckBecomesErrorOutputNotSuccess() {
    AtomicInteger sendInvocations = new AtomicInteger();
    when(mockProducer.send(any(ProducerRecord.class)))
        .thenAnswer(
            invocation ->
                sendInvocations.incrementAndGet() == 2
                    ? CompletableFuture.failedFuture(new RuntimeException("ack failed"))
                    : CompletableFuture.completedFuture(mock(RecordMetadata.class)));

    KafkaSinkCommand command = buildCommand(baseConfig().build());
    ScalarSinkCommand.SinkResult result =
        command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    assertEquals(EVENTS.size() - 1, result.getSuccessCount());
    assertEquals(1, result.errorCount());
  }

  @Test
  void defaultDeliveryMode_unreachableBrokerFailsBatchBeforeAnySend() {
    when(mockProducer.partitionsFor(TOPIC)).thenThrow(new TimeoutException("no metadata"));

    KafkaSinkCommand command = buildCommand(baseConfig().build());
    ScalarSinkCommand.SinkResult result =
        command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    assertEquals(0, result.getSuccessCount());
    assertEquals(EVENTS.size(), result.errorCount());
    verify(mockProducer, never()).send(any(ProducerRecord.class));
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  void configWithoutDeliveryModeField_defaultsToWaitForAck() {
    KafkaSinkCommand command =
        buildCommandFromConfigMap(
            Map.of("broker", "localhost:9092", "topic", TOPIC, "encodingType", "JSON_OBJECT"));

    command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    assertEquals("all", capturedProducerProperties.get(ProducerConfig.ACKS_CONFIG));
    verify(mockProducer).flush();
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void encoding_sendsEachRecordAsOneMessageInThatFormat() {
    KafkaSinkCommand command =
        buildCommandFromConfigMap(
            Map.of(
                "broker",
                "localhost:9092",
                "topic",
                TOPIC,
                "encoding",
                Map.of("format", Map.of("type", "csv", "delimiter", "|"))));

    command.writeToSink(
        List.of(
            (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "alice")),
            (RecordFleakData) FleakData.wrap(Map.of("id", 2, "name", "bob"))),
        "test_user",
        command.getExecutionContext());

    ArgumentCaptor<ProducerRecord<byte[], byte[]>> sent =
        ArgumentCaptor.forClass(ProducerRecord.class);
    verify(mockProducer, times(2)).send(sent.capture());
    assertEquals(
        List.of("name|id\r\nalice|1\r\n", "name|id\r\nbob|2\r\n"),
        sent.getAllValues().stream()
            .map(record -> new String(record.value(), StandardCharsets.UTF_8))
            .toList());
  }

  @Test
  void deferredConfirmation_flushReturnsBeforeAcksAndBarrierFlushesProducer() throws Exception {
    KafkaSinkCommand command = buildDeferredCommand(baseConfig().build());

    ScalarSinkCommand.SinkResult result =
        command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    assertEquals(EVENTS.size(), result.getSuccessCount());
    verify(mockProducer, times(EVENTS.size())).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockProducer, never()).send(any(ProducerRecord.class));
    verify(mockProducer, never()).flush();
    assertEquals("all", capturedProducerProperties.get(ProducerConfig.ACKS_CONFIG));

    command.awaitDeliveryConfirmation();

    verify(mockProducer).flush();
  }

  @Test
  void deferredConfirmation_connectivityFailureAfterSendFailsBarrier() {
    List<Callback> callbacks = captureCallbacksWithoutCompleting();
    KafkaSinkCommand command = buildDeferredCommand(baseConfig().build());
    command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    callbacks.getFirst().onCompletion(mock(RecordMetadata.class), null);
    callbacks.get(1).onCompletion(null, new TimeoutException("delivery timed out"));

    Exception thrown = assertThrows(Exception.class, command::awaitDeliveryConfirmation);
    assertInstanceOf(TimeoutException.class, thrown.getCause());
  }

  @Test
  void deferredConfirmation_permanentFailureAfterSendDoesNotFailBarrier() throws Exception {
    List<Callback> callbacks = captureCallbacksWithoutCompleting();
    KafkaSinkCommand command = buildDeferredCommand(baseConfig().build());
    command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    callbacks.getFirst().onCompletion(null, new RecordTooLargeException("too large"));

    command.awaitDeliveryConfirmation();
    verify(mockProducer).flush();
  }

  @Test
  void deferredConfirmation_rejectionDuringSendBecomesErrorOutputOnly() throws Exception {
    when(mockProducer.send(any(ProducerRecord.class), any(Callback.class)))
        .thenAnswer(
            invocation -> {
              TimeoutException rejection = new TimeoutException("no metadata after 60000 ms");
              Callback callback = invocation.getArgument(1);
              callback.onCompletion(null, rejection);
              return CompletableFuture.failedFuture(rejection);
            });
    KafkaSinkCommand command = buildDeferredCommand(baseConfig().build());

    ScalarSinkCommand.SinkResult result =
        command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    assertEquals(0, result.getSuccessCount());
    assertEquals(EVENTS.size(), result.errorCount());
    command.awaitDeliveryConfirmation();
  }

  @Test
  void deferredConfirmation_isIgnoredForFireAndForget() throws Exception {
    KafkaSinkCommand command =
        buildDeferredCommand(
            baseConfig().deliveryMode(KafkaSinkDto.DeliveryMode.FIRE_AND_FORGET).build());
    command.writeToSink(EVENTS, "test_user", command.getExecutionContext());

    command.awaitDeliveryConfirmation();

    assertEquals("1", capturedProducerProperties.get(ProducerConfig.ACKS_CONFIG));
    verify(mockProducer, never()).flush();
  }

  private List<Callback> captureCallbacksWithoutCompleting() {
    List<Callback> callbacks = new ArrayList<>();
    when(mockProducer.send(any(ProducerRecord.class), any(Callback.class)))
        .thenAnswer(
            invocation -> {
              callbacks.add(invocation.getArgument(1));
              return new CompletableFuture<RecordMetadata>();
            });
    return callbacks;
  }
}
