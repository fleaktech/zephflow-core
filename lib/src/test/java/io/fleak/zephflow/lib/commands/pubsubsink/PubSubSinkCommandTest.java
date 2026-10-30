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
package io.fleak.zephflow.lib.commands.pubsubsink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PubsubMessage;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.gcp.PubSubClientFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PubSubSinkCommandTest {

  @Test
  void encodingJsonObject_publishesTheDataOfEncodingType() throws Exception {
    RecordFleakData record =
        new RecordFleakData(
            Map.of(
                "city", new StringPrimitiveFleakData("Zürich 苏黎世"),
                "id", new StringPrimitiveFleakData("7")));
    PublishRequest expected =
        PublishRequest.newBuilder()
            .setTopic("projects/p/topics/t")
            .addMessages(
                PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8("{\"city\":\"Zürich 苏黎世\",\"id\":\"7\"}")))
            .build();

    assertEquals(expected, publishedRequest(Map.of("encodingType", "JSON_OBJECT"), record));
    assertEquals(
        expected,
        publishedRequest(
            Map.of("encoding", Map.of("format", Map.of("type", "json", "mode", "object"))),
            record));
  }

  private static PublishRequest publishedRequest(Map<String, Object> format, RecordFleakData record)
      throws Exception {
    PubSubClientFactory clients = mock();
    PublisherStub stub = mock();
    UnaryCallable<PublishRequest, PublishResponse> publish = mock();
    when(clients.createPublisherStub()).thenReturn(stub);
    when(stub.publishCallable()).thenReturn(publish);
    when(publish.call(any(PublishRequest.class)))
        .thenReturn(PublishResponse.newBuilder().addMessageIds("m").build());
    PubSubSinkCommand command =
        new PubSubSinkCommand(
            "node",
            TestUtils.JOB_CONTEXT,
            new JsonConfigParser<>(PubSubSinkDto.Config.class),
            new PubSubSinkConfigValidator(),
            clients);
    Map<String, Object> config = new HashMap<>(format);
    config.put("projectId", "p");
    config.put("topic", "t");
    command.parseAndValidateArg(config);
    try {
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      command.writeToSink(List.of(record), "user", command.getExecutionContext());
    } finally {
      command.terminate();
    }
    ArgumentCaptor<PublishRequest> sent = ArgumentCaptor.forClass(PublishRequest.class);
    verify(publish).call(sent.capture());
    return sent.getValue();
  }
}
