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

import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PubsubMessage;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class PubSubSinkFlusher implements SimpleSinkCommand.Flusher<PubSubOutboundMessage> {

  private final PublisherStub publisherStub;
  private final String topicPath;

  public PubSubSinkFlusher(PublisherStub publisherStub, String topicPath) {
    this.publisherStub = publisherStub;
    this.topicPath = topicPath;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {

    List<PubSubOutboundMessage> messages = preparedInputEvents.preparedList();
    if (messages.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    List<PubsubMessage> pubsubMessages = new ArrayList<>(messages.size());
    long flushedDataSize = 0;
    for (PubSubOutboundMessage message : messages) {
      PubsubMessage.Builder builder =
          PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(message.body()));
      if (message.orderingKey() != null) {
        builder.setOrderingKey(message.orderingKey());
      }
      pubsubMessages.add(builder.build());
      flushedDataSize += message.body().getBytes(StandardCharsets.UTF_8).length;
    }

    PublishRequest request =
        PublishRequest.newBuilder().setTopic(topicPath).addAllMessages(pubsubMessages).build();

    try {
      publisherStub.publishCallable().call(request);
      log.debug("Pub/Sub flush completed: {} messages to {}", messages.size(), topicPath);
      return new SimpleSinkCommand.FlushResult(messages.size(), flushedDataSize, List.of());
    } catch (Exception e) {
      log.error("Pub/Sub publish failed for topic {}", topicPath, e);
      List<ErrorOutput> errorOutputs = new ArrayList<>(messages.size());
      for (Pair<RecordFleakData, PubSubOutboundMessage> pair :
          preparedInputEvents.rawAndPreparedList()) {
        errorOutputs.add(
            new ErrorOutput(pair.getLeft(), "Pub/Sub publish failed: " + e.getMessage()));
      }
      return new SimpleSinkCommand.FlushResult(0, 0, errorOutputs);
    }
  }

  @Override
  public void close() {
    if (publisherStub != null) {
      publisherStub.close();
    }
  }
}
