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
package io.fleak.zephflow.lib.commands.pubsubsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.MessageDecodingConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.gcp.PubSubClientFactory;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * The converter this source's {@code createExecutionContext} builds: {@code decoding} alone decodes
 * messages, {@code encodingType} decodes a body as it is, even one that starts with the gzip magic
 * bytes, and a message that fails to decode is dead-lettered as its body with the decode text.
 */
class PubSubSourceDecodingTest {

  private static final DecodingConfig JSON_OBJECT =
      new DecodingConfig(List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT));

  @Test
  void decodingAloneDecodesMessages() throws Exception {
    SourceExecutionContext<PubSubReceivedMessage> context =
        context(
            config()
                .decoding(
                    new DecodingConfig(
                        List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.LINES)))
                .build());
    PubSubReceivedMessage message =
        new PubSubReceivedMessage(
            "{\"a\":1}\n{\"a\":2}".getBytes(StandardCharsets.UTF_8), "m-1", "a-1");

    assertEquals(
        ConvertedResult.success(List.of(record(1), record(2)), message),
        context.converter().convert(message, context));
  }

  @Test
  void decodingDecompressesWithItsCompression() throws Exception {
    SourceExecutionContext<PubSubReceivedMessage> context =
        context(
            config()
                .decoding(
                    new DecodingConfig(
                        List.of(DecompressionCodec.GZIP),
                        new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT)))
                .build());
    PubSubReceivedMessage message = new PubSubReceivedMessage(gzip("{\"a\":1}"), "m-1", "a-1");

    assertEquals(
        ConvertedResult.success(List.of(record(1)), message),
        context.converter().convert(message, context));
  }

  @Test
  void encodingTypeDecodesGzipBytesAsTheyAre() throws Exception {
    SourceExecutionContext<PubSubReceivedMessage> context =
        context(config().encodingType(EncodingType.JSON_OBJECT).build());
    PubSubReceivedMessage message = new PubSubReceivedMessage(gzip("{\"a\":1}"), "m-1", "a-1");

    ConvertedResult<PubSubReceivedMessage> result = context.converter().convert(message, context);

    assertEquals(
        Arrays.asList(null, message, "MALFORMED json message: JsonParseException"),
        Arrays.asList(
            result.transformedData(), result.sourceRecord(), result.error().getMessage()));
  }

  @Test
  void aBadMessageIsDeadLetteredAsItsBody() throws Exception {
    JobContext jobContext = TestUtils.buildJobContext(new HashMap<>());
    jobContext.setDlqConfig(JobContext.S3DlqConfig.builder().build());
    UnaryCallable<PullRequest, PullResponse> pull = mock();
    UnaryCallable<AcknowledgeRequest, Empty> ack = mock();
    SubscriberStub stub = mock();
    when(stub.pullCallable()).thenReturn(pull);
    when(stub.acknowledgeCallable()).thenReturn(ack);
    PubSubClientFactory clients = mock();
    when(clients.createSubscriberStub()).thenReturn(stub);
    PubSubSourceCommand command =
        new PubSubSourceCommand(
            "node",
            jobContext,
            new JsonConfigParser<>(PubSubSourceDto.Config.class),
            new PubSubSourceConfigValidator(),
            clients);
    when(pull.call(any(PullRequest.class)))
        .thenAnswer(
            i -> {
              command.terminate();
              return PullResponse.newBuilder()
                  .addReceivedMessages(message("m-1", "not json"))
                  .addReceivedMessages(message("m-2", "{\"a\":1}"))
                  .build();
            });
    command.parseAndValidateArg(
        OBJECT_MAPPER.convertValue(
            config().decoding(JSON_OBJECT).build(), new TypeReference<>() {}));
    DlqWriter dlqWriter = mock();
    SourceEventAcceptor acceptor = mock();
    List<String> errors;
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

    assertEquals(
        List.of(
            "failed to deserialize event: MALFORMED json message: JsonParseException\n"
                + "Pub/Sub messageId: m-1"),
        errors);

    ArgumentCaptor<SerializedEvent> value = ArgumentCaptor.forClass(SerializedEvent.class);
    ArgumentCaptor<String> text = ArgumentCaptor.forClass(String.class);
    verify(dlqWriter).writeToDlq(anyLong(), value.capture(), text.capture(), eq("node"));
    List<String> lines = text.getValue().lines().toList();
    assertEquals(
        List.of(
            new SerializedEvent(null, "not json".getBytes(StandardCharsets.UTF_8), null).toString(),
            "MALFORMED json message: JsonParseException",
            "Action: check that the message is json as the source's encodingType or decoding"
                + " says, and that its compression is set when the message is compressed, or fix"
                + " the producer that wrote it; the DLQ value is the message as received, so it"
                + " can be sent again",
            "com.fasterxml.jackson.core.JsonParseException",
            "bytes 0-8"),
        List.of(
            value.getValue().toString(),
            lines.get(0),
            lines.get(1),
            lines.get(2).split(":")[0],
            lines.getLast()));
    verify(acceptor).accept(List.of(record(1)));
    verify(ack)
        .call(
            AcknowledgeRequest.newBuilder()
                .setSubscription("projects/p/subscriptions/s")
                .addAllAckIds(List.of("a-m-1", "a-m-2"))
                .build());
  }

  private static PubSubSourceDto.Config.ConfigBuilder config() {
    return PubSubSourceDto.Config.builder().projectId("p").subscription("s");
  }

  @SuppressWarnings("unchecked")
  private static SourceExecutionContext<PubSubReceivedMessage> context(PubSubSourceDto.Config built)
      throws Exception {
    PubSubSourceDto.Config config =
        new JsonConfigParser<>(PubSubSourceDto.Config.class)
            .parseConfig(OBJECT_MAPPER.convertValue(built, new TypeReference<>() {}));
    new PubSubSourceConfigValidator().validateConfig(config, "node", TestUtils.JOB_CONTEXT);
    PubSubClientFactory clients = mock();
    when(clients.createSubscriberStub()).thenReturn(mock(SubscriberStub.class));
    PubSubSourceCommand command =
        new PubSubSourceCommand(
            "node",
            TestUtils.JOB_CONTEXT,
            new JsonConfigParser<>(PubSubSourceDto.Config.class),
            new PubSubSourceConfigValidator(),
            clients);
    return (SourceExecutionContext<PubSubReceivedMessage>)
        command.createExecutionContext(
            new MetricClientProvider.NoopMetricClientProvider(),
            TestUtils.JOB_CONTEXT,
            config,
            "node");
  }

  private static ReceivedMessage message(String id, String body) {
    return ReceivedMessage.newBuilder()
        .setAckId("a-" + id)
        .setMessage(
            PubsubMessage.newBuilder().setMessageId(id).setData(ByteString.copyFromUtf8(body)))
        .build();
  }

  private static RecordFleakData record(int a) {
    return (RecordFleakData) FleakData.wrap(Map.of("a", a));
  }

  private static byte[] gzip(String text) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(text.getBytes(StandardCharsets.UTF_8));
    }
    return out.toByteArray();
  }
}
