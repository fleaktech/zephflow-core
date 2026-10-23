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
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubClientFactory;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.MessageDecodingConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.GZIPOutputStream;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

/**
 * The converter this source's {@code createExecutionContext} builds: {@code decoding} alone decodes
 * messages, {@code encodingType} decodes a body as it is, even one that starts with the gzip magic
 * bytes, and a message that fails to decode is dead-lettered as received with the decode text.
 */
class AzureIotHubSourceDecodingTest {

  private static final Instant ENQUEUED = Instant.parse("2026-07-16T10:00:00Z");

  @Test
  void decodingAloneDecodesMessagesWithItsCompression() throws Exception {
    SourceExecutionContext<SerializedEvent> context =
        context(
            config()
                .decoding(
                    new DecodingConfig(
                        List.of(DecompressionCodec.GZIP),
                        new JsonFormatConfig(JsonFormatConfig.Mode.LINES)))
                .build());
    SerializedEvent message = new SerializedEvent(null, gzip("{\"a\":1}\n{\"a\":2}"), null);

    assertEquals(
        ConvertedResult.success(List.of(record(1), record(2)), message),
        context.converter().convert(message, context));
  }

  @Test
  void encodingTypeDecodesGzipBytesAsTheyAre() throws Exception {
    SourceExecutionContext<SerializedEvent> context =
        context(config().encodingType(EncodingType.JSON_OBJECT).build());
    SerializedEvent message = new SerializedEvent(null, gzip("{\"a\":1}"), null);

    ConvertedResult<SerializedEvent> result = context.converter().convert(message, context);

    assertEquals(
        Arrays.asList(null, message, "MALFORMED json message: JsonParseException"),
        Arrays.asList(
            result.transformedData(), result.sourceRecord(), result.error().getMessage()));
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aBadMessageIsDeadLetteredAsReceived() throws Exception {
    JobContext jobContext = TestUtils.buildJobContext(new HashMap<>());
    jobContext.setDlqConfig(JobContext.S3DlqConfig.builder().build());
    AzureEventHubClientFactory clients = mock();
    when(clients.createProcessorClient(any(), any(), any(), any(), any(), any()))
        .thenAnswer(
            i -> {
              Consumer<EventContext> onEvent = i.getArgument(4);
              onEvent.accept(event("not json"));
              onEvent.accept(event("{\"a\":1}"));
              return mock(EventProcessorClient.class);
            });
    AzureIotHubSourceCommand command =
        new AzureIotHubSourceCommand(
            "node",
            jobContext,
            new JsonConfigParser<>(AzureIotHubSourceDto.Config.class),
            new AzureIotHubSourceConfigValidator(),
            clients);
    command.parseAndValidateArg(
        OBJECT_MAPPER.convertValue(
            config()
                .decoding(
                    new DecodingConfig(
                        List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT)))
                .build(),
            new TypeReference<>() {}));
    DlqWriter dlqWriter = mock();
    SourceEventAcceptor acceptor = mock();
    List<String> errors;
    doAnswer(
            i -> {
              command.terminate();
              return null;
            })
        .when(acceptor)
        .accept(any());
    try (var dlqFactory = mockStatic(DlqWriterFactory.class)) {
      dlqFactory
          .when(() -> DlqWriterFactory.createDlqWriter(jobContext.getDlqConfig(), null))
          .thenReturn(dlqWriter);
      dlqFactory
          .when(() -> DlqWriterFactory.createSampleWriter(jobContext.getDlqConfig(), null))
          .thenReturn(mock(DlqWriter.class));
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      try (BoundedLogCapture logs = new BoundedLogCapture(MessageDecodingConverter.class)) {
        command.execute("user", acceptor);
        errors =
            logs.events().stream()
                .filter(e -> e.getLevel() == Level.ERROR)
                .map(e -> e.getMessage().getFormattedMessage())
                .toList();
      }
    }

    ArgumentCaptor<SerializedEvent> value = ArgumentCaptor.forClass(SerializedEvent.class);
    ArgumentCaptor<String> text = ArgumentCaptor.forClass(String.class);
    verify(dlqWriter).writeToDlq(anyLong(), value.capture(), text.capture(), eq("node"));
    List<String> lines = text.getValue().lines().toList();
    assertEquals(
        List.of(
            "device-key",
            "not json",
            Map.of(
                "deviceId", "sensor-01", "enqueuedTime", ENQUEUED.toString(), "prop.schema", "v2"),
            "MALFORMED json message: JsonParseException",
            "Action: check that the message is json as the source's encodingType or decoding"
                + " says, and that its compression is set when the message is compressed, or fix"
                + " the producer that wrote it; the DLQ value is the message as received, so it"
                + " can be sent again",
            "com.fasterxml.jackson.core.JsonParseException",
            "bytes 0-8"),
        List.of(
            new String(value.getValue().key(), StandardCharsets.UTF_8),
            new String(value.getValue().value(), StandardCharsets.UTF_8),
            value.getValue().metadata(),
            lines.get(0),
            lines.get(1),
            lines.get(2).split(":")[0],
            lines.getLast()));
    assertEquals(
        List.of(
            "failed to deserialize event: MALFORMED json message: JsonParseException\n"
                + value.getValue()),
        errors);
    verify(acceptor)
        .accept(
            List.of(
                record(
                    Map.of(
                        "a",
                        1,
                        "deviceId",
                        "sensor-01",
                        "enqueuedTime",
                        ENQUEUED.toString(),
                        "prop.schema",
                        "v2",
                        "__md_key",
                        "ZGV2aWNlLWtleQ==",
                        "_iothub",
                        Map.of(
                            "deviceId",
                            "sensor-01",
                            "enqueuedTime",
                            ENQUEUED.toString(),
                            "properties",
                            Map.of("schema", "v2"))))));
  }

  private static EventContext event(String body) {
    EventData data = mock();
    when(data.getBody()).thenReturn(body.getBytes(StandardCharsets.UTF_8));
    when(data.getPartitionKey()).thenReturn("device-key");
    when(data.getSystemProperties())
        .thenReturn(Map.of(IotHubMetadataExtractor.DEVICE_ID_SYSTEM_PROPERTY, "sensor-01"));
    when(data.getEnqueuedTime()).thenReturn(ENQUEUED);
    when(data.getProperties()).thenReturn(Map.of("schema", "v2"));
    EventContext context = mock();
    when(context.getEventData()).thenReturn(data);
    when(context.getPartitionContext())
        .thenReturn(new PartitionContext("ns", "hub", "$Default", "0"));
    return context;
  }

  private static AzureIotHubSourceDto.Config.ConfigBuilder config() {
    return AzureIotHubSourceDto.Config.builder()
        .connectionString("Endpoint=sb://ns/;SharedAccessKeyName=k;SharedAccessKey=v")
        .eventHubName("hub")
        .checkpointStorageConnectionString("UseDevelopmentStorage=true")
        .checkpointContainerName("checkpoints");
  }

  @SuppressWarnings("unchecked")
  private static SourceExecutionContext<SerializedEvent> context(AzureIotHubSourceDto.Config built)
      throws Exception {
    AzureIotHubSourceDto.Config config =
        new JsonConfigParser<>(AzureIotHubSourceDto.Config.class)
            .parseConfig(OBJECT_MAPPER.convertValue(built, new TypeReference<>() {}));
    new AzureIotHubSourceConfigValidator().validateConfig(config, "node", TestUtils.JOB_CONTEXT);
    AzureEventHubClientFactory clients = mock();
    when(clients.createProcessorClient(any(), any(), any(), any(), any(), any()))
        .thenReturn(mock(EventProcessorClient.class));
    AzureIotHubSourceCommand command =
        new AzureIotHubSourceCommand(
            "node",
            TestUtils.JOB_CONTEXT,
            new JsonConfigParser<>(AzureIotHubSourceDto.Config.class),
            new AzureIotHubSourceConfigValidator(),
            clients);
    return (SourceExecutionContext<SerializedEvent>)
        command.createExecutionContext(
            new MetricClientProvider.NoopMetricClientProvider(),
            TestUtils.JOB_CONTEXT,
            config,
            "node");
  }

  private static RecordFleakData record(int a) {
    return (RecordFleakData) FleakData.wrap(Map.of("a", a));
  }

  private static RecordFleakData record(Map<String, Object> fields) {
    return (RecordFleakData) FleakData.wrap(fields);
  }

  private static byte[] gzip(String text) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(text.getBytes(StandardCharsets.UTF_8));
    }
    return out.toByteArray();
  }
}
