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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.PerRecordCommitStrategy;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * Long-polls SQS for S3 event notifications and turns each message into an {@link S3EventMessage}
 * (object references only — no S3 I/O happens here). Enforces an in-app retry cap based on the
 * message's {@code ApproximateReceiveCount}: once exceeded the message is dead-lettered and deleted
 * rather than reprocessed.
 */
@Slf4j
public class S3RealtimeSourceFetcher implements Fetcher<S3EventMessage> {

  private final SqsClient sqsClient;
  // Held only so the shared S3 client is closed when this fetcher is closed by the execution
  // context; the actual S3 reads happen in S3RealtimeRawDataConverter.
  private final S3Client s3Client;
  private final String queueUrl;
  private final int maxNumberOfMessages;
  private final int waitTimeSeconds;
  private final int visibilityTimeoutSeconds;
  private final int maxRetries;
  private final DlqWriter dlqWriter;
  private final String nodeId;

  // Receipt handles of messages whose records were successfully converted+emitted. The converter
  // adds to this queue on success; the committer drains it. Failed messages are never added, so
  // they are left undeleted for SQS redelivery.
  private final Queue<String> confirmedReceiptHandles;

  public S3RealtimeSourceFetcher(
      SqsClient sqsClient,
      S3Client s3Client,
      String queueUrl,
      int maxNumberOfMessages,
      int waitTimeSeconds,
      int visibilityTimeoutSeconds,
      int maxRetries,
      DlqWriter dlqWriter,
      String nodeId,
      Queue<String> confirmedReceiptHandles) {
    this.sqsClient = sqsClient;
    this.s3Client = s3Client;
    this.queueUrl = queueUrl;
    this.maxNumberOfMessages = maxNumberOfMessages;
    this.waitTimeSeconds = waitTimeSeconds;
    this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
    this.maxRetries = maxRetries;
    this.dlqWriter = dlqWriter;
    this.nodeId = nodeId;
    this.confirmedReceiptHandles = confirmedReceiptHandles;
  }

  @Override
  public List<S3EventMessage> fetch() {
    ReceiveMessageRequest request =
        ReceiveMessageRequest.builder()
            .queueUrl(queueUrl)
            .maxNumberOfMessages(maxNumberOfMessages)
            .waitTimeSeconds(waitTimeSeconds)
            .visibilityTimeout(visibilityTimeoutSeconds)
            .messageSystemAttributeNames(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT)
            .build();

    ReceiveMessageResponse response = sqsClient.receiveMessage(request);
    List<Message> messages = response.messages();
    if (messages == null || messages.isEmpty()) {
      return List.of();
    }

    List<S3EventMessage> result = new ArrayList<>(messages.size());
    for (Message message : messages) {
      List<S3ObjectRef> refs;
      try {
        refs = S3EventNotificationParser.parse(message.body());
      } catch (Exception e) {
        // Malformed envelope can never succeed; dead-letter and remove it.
        deadLetter(message, "failed to parse S3 event notification: " + e.getMessage());
        deleteMessage(message.receiptHandle());
        continue;
      }

      if (refs.isEmpty()) {
        // s3:TestEvent or non-ObjectCreated event: nothing to process, just acknowledge.
        log.debug("skipping SQS message {} with no ObjectCreated records", message.messageId());
        deleteMessage(message.receiptHandle());
        continue;
      }

      if (receiveCount(message) > maxRetries) {
        deadLetter(message, "exceeded maxRetries=" + maxRetries);
        deleteMessage(message.receiptHandle());
        continue;
      }

      result.add(new S3EventMessage(message.messageId(), message.receiptHandle(), refs));
    }

    log.debug("fetched {} S3 event messages from queue {}", result.size(), queueUrl);
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
      while ((receiptHandle = confirmedReceiptHandles.poll()) != null) {
        deleteMessage(receiptHandle);
      }
    };
  }

  @Override
  public CommitStrategy commitStrategy() {
    return PerRecordCommitStrategy.INSTANCE;
  }

  @Override
  public void close() {
    if (sqsClient != null) {
      sqsClient.close();
    }
    if (s3Client != null) {
      s3Client.close();
    }
  }

  private int receiveCount(Message message) {
    String value = message.attributes().get(MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT);
    if (value == null) {
      return 1;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return 1;
    }
  }

  private void deadLetter(Message message, String errorMsg) {
    if (dlqWriter == null) {
      log.warn("dropping SQS message {} (no DLQ configured): {}", message.messageId(), errorMsg);
      return;
    }
    SerializedEvent raw =
        new SerializedEvent(
            message.messageId() == null
                ? null
                : message.messageId().getBytes(StandardCharsets.UTF_8),
            message.body().getBytes(StandardCharsets.UTF_8),
            null);
    dlqWriter.writeToDlq(System.currentTimeMillis(), raw, errorMsg, nodeId);
  }

  private void deleteMessage(String receiptHandle) {
    try {
      sqsClient.deleteMessage(
          DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(receiptHandle).build());
    } catch (Exception e) {
      log.error("failed to delete SQS message with receiptHandle: {}", receiptHandle, e);
    }
  }
}
