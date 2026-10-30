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
package io.fleak.zephflow.lib.commands.sqssink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

class SqsSinkCommandTest {

  private static class TestableSqsSinkCommand extends SqsSinkCommand {
    TestableSqsSinkCommand() {
      super(null, null, null, null, null);
    }

    void setConfig(SqsSinkDto.Config config) {
      this.commandConfig = config;
    }

    int callBatchSize() {
      return batchSize();
    }
  }

  private SqsSinkDto.Config config(Integer batchSize) {
    return SqsSinkDto.Config.builder()
        .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue")
        .regionStr("us-east-1")
        .encodingType("JSON_OBJECT")
        .batchSize(batchSize)
        .build();
  }

  @Test
  void batchSize_honorsConfiguredValue() {
    var cmd = new TestableSqsSinkCommand();
    cmd.setConfig(config(3));
    assertEquals(3, cmd.callBatchSize());
  }

  @Test
  void batchSize_clampsAboveMaxToMax() {
    var cmd = new TestableSqsSinkCommand();
    cmd.setConfig(config(50));
    assertEquals(SqsSinkDto.MAX_BATCH_SIZE, cmd.callBatchSize());
  }

  @Test
  void batchSize_clampsNonPositiveToOne() {
    var cmd = new TestableSqsSinkCommand();
    cmd.setConfig(config(0));
    assertEquals(1, cmd.callBatchSize());
  }

  @Test
  void batchSize_defaultsWhenNull() {
    var cmd = new TestableSqsSinkCommand();
    cmd.setConfig(config(null));
    assertEquals(SqsSinkDto.DEFAULT_BATCH_SIZE, cmd.callBatchSize());
  }

  private static final String QUEUE_URL =
      "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";

  @Test
  void encodingJsonObject_sendsTheBodyOfEncodingType() throws Exception {
    RecordFleakData record =
        new RecordFleakData(
            Map.of(
                "city", new StringPrimitiveFleakData("Zürich 苏黎世"),
                "id", new StringPrimitiveFleakData("7")));
    SendMessageBatchRequest expected =
        SendMessageBatchRequest.builder()
            .queueUrl(QUEUE_URL)
            .entries(
                SendMessageBatchRequestEntry.builder()
                    .id("0")
                    .messageBody("{\"city\":\"Zürich 苏黎世\",\"id\":\"7\"}")
                    .build())
            .build();

    assertEquals(expected, sentRequest(Map.of("encodingType", "JSON_OBJECT"), record));
    assertEquals(
        expected,
        sentRequest(
            Map.of("encoding", Map.of("format", Map.of("type", "json", "mode", "object"))),
            record));
  }

  private static SendMessageBatchRequest sentRequest(
      Map<String, Object> format, RecordFleakData record) throws Exception {
    AwsClientFactory clients = mock();
    SqsClient sqs = mock();
    when(clients.createSqsClient(any(), any())).thenReturn(sqs);
    when(sqs.sendMessageBatch(any(SendMessageBatchRequest.class)))
        .thenReturn(
            SendMessageBatchResponse.builder()
                .successful(SendMessageBatchResultEntry.builder().id("0").messageId("m").build())
                .build());
    SqsSinkCommand command =
        new SqsSinkCommand(
            "node",
            TestUtils.JOB_CONTEXT,
            new JsonConfigParser<>(SqsSinkDto.Config.class),
            new SqsSinkConfigValidator(),
            clients);
    Map<String, Object> config = new HashMap<>(format);
    config.put("queueUrl", QUEUE_URL);
    config.put("regionStr", "us-east-1");
    command.parseAndValidateArg(config);
    try {
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      command.writeToSink(List.of(record), "user", command.getExecutionContext());
    } finally {
      command.terminate();
    }
    ArgumentCaptor<SendMessageBatchRequest> sent =
        ArgumentCaptor.forClass(SendMessageBatchRequest.class);
    verify(sqs).sendMessageBatch(sent.capture());
    return sent.getValue();
  }
}
