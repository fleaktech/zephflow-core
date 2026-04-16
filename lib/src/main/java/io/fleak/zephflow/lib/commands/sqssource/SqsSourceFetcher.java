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

import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.PerRecordCommitStrategy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

@Slf4j
public class SqsSourceFetcher implements Fetcher<SqsReceivedMessage> {

  private final SqsClient sqsClient;
  private final String queueUrl;
  private final int maxNumberOfMessages;
  private final int waitTimeSeconds;
  private final int visibilityTimeoutSeconds;
  private final ConcurrentLinkedQueue<String> pendingReceiptHandles = new ConcurrentLinkedQueue<>();

  public SqsSourceFetcher(
      SqsClient sqsClient,
      String queueUrl,
      int maxNumberOfMessages,
      int waitTimeSeconds,
      int visibilityTimeoutSeconds) {
    this.sqsClient = sqsClient;
    this.queueUrl = queueUrl;
    this.maxNumberOfMessages = maxNumberOfMessages;
    this.waitTimeSeconds = waitTimeSeconds;
    this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
  }

  @Override
  public List<SqsReceivedMessage> fetch() {
    ReceiveMessageRequest request =
        ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .maxNumberOfMessages(maxNumberOfMessages)
            .waitTimeSeconds(waitTimeSeconds)
            .visibilityTimeout(visibilityTimeoutSeconds)
            .attributeNamesWithStrings("All")
            .messageAttributeNames("All")
            .build();

    ReceiveMessageResponse response = sqsClient.receiveMessage(request);
    List<Message> messages = response.messages();

    if (messages == null || messages.isEmpty()) {
      log.debug("No messages received from SQS queue: {}", queueUrl);
      return List.of();
    }

    List<SqsReceivedMessage> result = new ArrayList<>(messages.size());
    for (Message message : messages) {
      Map<String, String> attributes = new HashMap<>();
      if (message.attributesAsStrings() != null) {
        attributes.putAll(message.attributesAsStrings());
      }
      if (message.messageAttributes() != null) {
        message
            .messageAttributes()
            .forEach(
                (key, value) -> {
                  if (value.stringValue() != null) {
                    attributes.put(key, value.stringValue());
                  }
                });
      }

      SqsReceivedMessage receivedMessage =
          new SqsReceivedMessage(
              message.body().getBytes(StandardCharsets.UTF_8),
              message.messageId(),
              message.receiptHandle(),
              attributes);
      pendingReceiptHandles.add(message.receiptHandle());
      result.add(receivedMessage);
    }

    log.debug("Fetched {} messages from SQS queue: {}", result.size(), queueUrl);
    return result;
  }

  @Override
  public boolean isExhausted() {
    return false;
  }

  @Override
  public Committer committer() {
    return () -> {
      String receiptHandle;
      while ((receiptHandle = pendingReceiptHandles.poll()) != null) {
        try {
          DeleteMessageRequest deleteRequest =
              DeleteMessageRequest.builder()
                  .queueUrl(queueUrl)
                  .receiptHandle(receiptHandle)
                  .build();
          sqsClient.deleteMessage(deleteRequest);
        } catch (Exception e) {
          log.error("Failed to delete SQS message with receiptHandle: {}", receiptHandle, e);
        }
      }
    };
  }

  @Override
  public CommitStrategy commitStrategy() {
    return PerRecordCommitStrategy.INSTANCE;
  }

  @Override
  public void close() throws IOException {
    if (sqsClient != null) {
      sqsClient.close();
    }
  }
}
