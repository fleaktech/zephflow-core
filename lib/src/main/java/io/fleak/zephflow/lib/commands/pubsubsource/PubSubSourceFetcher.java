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

import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.PerRecordCommitStrategy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PubSubSourceFetcher implements Fetcher<PubSubReceivedMessage> {

  private static final int ACK_CHUNK_SIZE = 1000;

  private final SubscriberStub subscriberStub;
  private final String subscriptionPath;
  private final int maxMessages;
  private final boolean returnImmediately;
  private final int ackDeadlineExtensionSeconds;
  private final ConcurrentLinkedQueue<String> pendingAckIds = new ConcurrentLinkedQueue<>();

  public PubSubSourceFetcher(
      SubscriberStub subscriberStub,
      String subscriptionPath,
      int maxMessages,
      boolean returnImmediately,
      int ackDeadlineExtensionSeconds) {
    this.subscriberStub = subscriberStub;
    this.subscriptionPath = subscriptionPath;
    this.maxMessages = maxMessages;
    this.returnImmediately = returnImmediately;
    this.ackDeadlineExtensionSeconds = ackDeadlineExtensionSeconds;
  }

  @Override
  public List<PubSubReceivedMessage> fetch() {
    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription(subscriptionPath)
            .setMaxMessages(maxMessages)
            .setReturnImmediately(returnImmediately)
            .build();

    PullResponse response = subscriberStub.pullCallable().call(request);
    List<ReceivedMessage> received = response.getReceivedMessagesList();

    if (received.isEmpty()) {
      log.debug("No messages received from Pub/Sub subscription: {}", subscriptionPath);
      return List.of();
    }

    if (ackDeadlineExtensionSeconds > 0) {
      List<String> ackIds = received.stream().map(ReceivedMessage::getAckId).toList();
      try {
        subscriberStub
            .modifyAckDeadlineCallable()
            .call(
                ModifyAckDeadlineRequest.newBuilder()
                    .setSubscription(subscriptionPath)
                    .addAllAckIds(ackIds)
                    .setAckDeadlineSeconds(ackDeadlineExtensionSeconds)
                    .build());
      } catch (Exception e) {
        log.warn(
            "Failed to extend ack deadline for {} messages on subscription {}",
            ackIds.size(),
            subscriptionPath,
            e);
      }
    }

    List<PubSubReceivedMessage> result = new ArrayList<>(received.size());
    for (ReceivedMessage rm : received) {
      PubsubMessage message = rm.getMessage();
      Map<String, String> attributes = new HashMap<>(message.getAttributesMap());
      attributes.put("messageId", message.getMessageId());
      if (!message.getOrderingKey().isEmpty()) {
        attributes.put("orderingKey", message.getOrderingKey());
      }
      if (message.hasPublishTime()) {
        attributes.put(
            "publishTime", com.google.protobuf.util.Timestamps.toString(message.getPublishTime()));
      }

      result.add(
          new PubSubReceivedMessage(
              message.getData().toByteArray(), message.getMessageId(), rm.getAckId(), attributes));
      pendingAckIds.add(rm.getAckId());
    }

    log.debug("Fetched {} messages from Pub/Sub subscription: {}", result.size(), subscriptionPath);
    return result;
  }

  @Override
  public boolean isExhausted() {
    return false;
  }

  @Override
  public Committer committer() {
    return () -> {
      List<String> ackIds = new ArrayList<>();
      String ackId;
      while ((ackId = pendingAckIds.poll()) != null) {
        ackIds.add(ackId);
        if (ackIds.size() >= ACK_CHUNK_SIZE) {
          ackChunk(ackIds);
          ackIds.clear();
        }
      }
      if (!ackIds.isEmpty()) {
        ackChunk(ackIds);
      }
    };
  }

  private void ackChunk(List<String> ackIds) {
    try {
      subscriberStub
          .acknowledgeCallable()
          .call(
              AcknowledgeRequest.newBuilder()
                  .setSubscription(subscriptionPath)
                  .addAllAckIds(ackIds)
                  .build());
    } catch (Exception e) {
      log.error(
          "Failed to acknowledge {} Pub/Sub messages on subscription {}",
          ackIds.size(),
          subscriptionPath,
          e);
    }
  }

  @Override
  public CommitStrategy commitStrategy() {
    return PerRecordCommitStrategy.INSTANCE;
  }

  @Override
  public void close() throws IOException {
    if (subscriberStub != null) {
      subscriberStub.close();
    }
  }
}
