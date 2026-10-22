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
package io.fleak.zephflow.lib.commands.sqssource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * The converter this source's {@code createExecutionContext} builds: {@code decoding} alone decodes
 * messages, {@code encodingType} decodes a body as it is, even one that starts with the gzip magic
 * bytes, and a message that fails to decode is dead-lettered as its body with the decode text.
 */
class SqsSourceDecodingTest {

  private static final DecodingConfig JSON_OBJECT =
      new DecodingConfig(List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT));

  @Test
  void decodingAloneDecodesMessages() throws Exception {
    SourceExecutionContext<SqsReceivedMessage> context =
        context(
            config()
                .decoding(
                    new DecodingConfig(
                        List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.LINES)))
                .build());
    SqsReceivedMessage message =
        new SqsReceivedMessage(
            "{\"a\":1}\n{\"a\":2}".getBytes(StandardCharsets.UTF_8), "m-1", "r-1");

    assertEquals(
        ConvertedResult.success(List.of(record(1), record(2)), message),
        context.converter().convert(message, context));
  }

  @Test
  void encodingTypeDecodesGzipBytesAsTheyAre() throws Exception {
    SourceExecutionContext<SqsReceivedMessage> context =
        context(config().encodingType(EncodingType.JSON_OBJECT).build());
    SqsReceivedMessage message = new SqsReceivedMessage(gzip("{\"a\":1}"), "m-1", "r-1");

    ConvertedResult<SqsReceivedMessage> result = context.converter().convert(message, context);

    assertEquals(
        Arrays.asList(null, message, "MALFORMED json message: JsonParseException"),
        Arrays.asList(
            result.transformedData(), result.sourceRecord(), result.error().getMessage()));
  }

  @Test
  void aBadMessageIsDeadLetteredAsItsBody() throws Exception {
    JobContext jobContext = TestUtils.buildJobContext(new HashMap<>());
    jobContext.setDlqConfig(JobContext.S3DlqConfig.builder().build());
    AwsClientFactory clients = mock();
    SqsClient sqs = mock();
    when(clients.createSqsClient(any(), any())).thenReturn(sqs);
    SqsSourceCommand command =
        new SqsSourceCommand(
            "node",
            jobContext,
            new JsonConfigParser<>(SqsSourceDto.Config.class),
            new SqsSourceConfigValidator(),
            clients);
    when(sqs.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenAnswer(
            i -> {
              command.terminate();
              return ReceiveMessageResponse.builder()
                  .messages(message("m-1", "not json"), message("m-2", "{\"a\":1}"))
                  .build();
            });
    command.parseAndValidateArg(
        OBJECT_MAPPER.convertValue(
            config().decoding(JSON_OBJECT).build(), new TypeReference<>() {}));
    DlqWriter dlqWriter = mock();
    SourceEventAcceptor acceptor = mock();
    try (var dlqFactory = mockStatic(DlqWriterFactory.class)) {
      dlqFactory
          .when(() -> DlqWriterFactory.createDlqWriter(jobContext.getDlqConfig(), null))
          .thenReturn(dlqWriter);
      dlqFactory
          .when(() -> DlqWriterFactory.createSampleWriter(jobContext.getDlqConfig(), null))
          .thenReturn(mock(DlqWriter.class));
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      command.execute("user", acceptor);
    }

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
    verify(sqs, times(2)).deleteMessage(any(DeleteMessageRequest.class));
  }

  private static SqsSourceDto.Config.ConfigBuilder config() {
    return SqsSourceDto.Config.builder()
        .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/q")
        .regionStr("us-east-1");
  }

  @SuppressWarnings("unchecked")
  private static SourceExecutionContext<SqsReceivedMessage> context(SqsSourceDto.Config built)
      throws Exception {
    SqsSourceDto.Config config =
        new JsonConfigParser<>(SqsSourceDto.Config.class)
            .parseConfig(OBJECT_MAPPER.convertValue(built, new TypeReference<>() {}));
    new SqsSourceConfigValidator().validateConfig(config, "node", TestUtils.JOB_CONTEXT);
    AwsClientFactory clients = mock();
    when(clients.createSqsClient(any(), any())).thenReturn(mock(SqsClient.class));
    SqsSourceCommand command =
        new SqsSourceCommand(
            "node",
            TestUtils.JOB_CONTEXT,
            new JsonConfigParser<>(SqsSourceDto.Config.class),
            new SqsSourceConfigValidator(),
            clients);
    return (SourceExecutionContext<SqsReceivedMessage>)
        command.createExecutionContext(
            new MetricClientProvider.NoopMetricClientProvider(),
            TestUtils.JOB_CONTEXT,
            config,
            "node");
  }

  private static Message message(String id, String body) {
    return Message.builder().messageId(id).receiptHandle("r-" + id).body(body).build();
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
