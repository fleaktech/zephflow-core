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

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.PerRecordCommitStrategy;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;
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
  private final S3RegionalClientProvider s3Clients;
  private final String queueUrl;
  private final int maxNumberOfMessages;
  private final int waitTimeSeconds;
  private final int visibilityTimeoutSeconds;
  private final int maxRetries;
  private final DlqWriter dlqWriter;
  private final String nodeId;
  private final FleakCounter deserializeFailureCounter;

  // Receipt handles of messages whose records were successfully converted+emitted. The converter
  // adds to this queue on success; the committer drains it. Failed messages are never added, so
  // they are left undeleted for SQS redelivery.
  private final Queue<String> confirmedReceiptHandles;

  public S3RealtimeSourceFetcher(
      SqsClient sqsClient,
      S3RegionalClientProvider s3Clients,
      String queueUrl,
      int maxNumberOfMessages,
      int waitTimeSeconds,
      int visibilityTimeoutSeconds,
      int maxRetries,
      DlqWriter dlqWriter,
      String nodeId,
      FleakCounter deserializeFailureCounter,
      Queue<String> confirmedReceiptHandles) {
    this.sqsClient = sqsClient;
    this.s3Clients = s3Clients;
    this.queueUrl = queueUrl;
    this.maxNumberOfMessages = maxNumberOfMessages;
    this.waitTimeSeconds = waitTimeSeconds;
    this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
    this.maxRetries = maxRetries;
    this.dlqWriter = dlqWriter;
    this.nodeId = nodeId;
    this.deserializeFailureCounter = deserializeFailureCounter;
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
      } catch (S3EventNotificationParser.NotS3NotificationException e) {
        reject(
            message,
            "NOT_S3_NOTIFICATION sqs message\nAction: this queue receives messages that are not S3"
                + " event notifications: to read ordinary events from it use the AWS SQS source"
                + " instead; for S3 events routed through EventBridge, send S3 event notifications"
                + " to this queue directly or through SNS; otherwise stop whatever sends them here\n"
                + e.getMessage());
        continue;
      } catch (Exception e) {
        reject(
            message,
            "MALFORMED s3 notification\nAction: check that only S3 event notifications in S3's JSON format are sent to"
                + " this queue, and fix what sends the others\nfailed to parse S3 event"
                + " notification: "
                + e.getMessage());
        continue;
      }

      if (refs.isEmpty()) {
        // s3:TestEvent or non-ObjectCreated event: nothing to process, just acknowledge.
        log.debug("skipping SQS message {} with no ObjectCreated records", message.messageId());
        deleteMessage(message.receiptHandle());
        continue;
      }

      if (receiveCount(message) > maxRetries) {
        String retriesError =
            "RETRIES_EXCEEDED s3 notification\nAction: the message was received more than"
                + " maxRetries times without finishing: check this source's earlier DLQ entries"
                + " and logs for why, raise visibilityTimeoutSeconds so a whole receive finishes"
                + " in time (or lower maxNumberOfMessages), or raise maxRetries; then send this"
                + " notification again\nreceived "
                + receiveCount(message)
                + " times, over maxRetries="
                + maxRetries;
        if (!deadLetter(message, retriesError)) {
          log.warn(
              "dropping SQS message {} (no DLQ configured): {}", message.messageId(), retriesError);
        }
        deleteMessage(message.receiptHandle());
        continue;
      }

      result.add(
          new S3EventMessage(message.messageId(), message.receiptHandle(), message.body(), refs));
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
    RuntimeException failure = null;
    try {
      if (sqsClient != null) {
        sqsClient.close();
      }
    } catch (RuntimeException e) {
      failure = new IllegalStateException("failed to close SQS client for node " + nodeId, e);
    }
    try {
      if (s3Clients != null) {
        s3Clients.close();
      }
    } catch (RuntimeException e) {
      RuntimeException s3Failure =
          new IllegalStateException("failed to close S3 clients for node " + nodeId, e);
      if (failure == null) {
        failure = s3Failure;
      } else {
        failure.addSuppressed(s3Failure);
      }
    }
    if (failure != null) {
      throw failure;
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

  // Counted so the panel shows an error; without a DLQ the message stays queued for redelivery.
  private void reject(Message message, String errorMsg) {
    deserializeFailureCounter.increase(Map.of());
    if (deadLetter(message, errorMsg)) {
      log.warn("dead-lettered SQS message {}: {}", message.messageId(), errorMsg);
      deleteMessage(message.receiptHandle());
    } else {
      log.warn(
          "keeping SQS message {} in the queue (no DLQ configured): {}",
          message.messageId(),
          errorMsg);
    }
  }

  private boolean deadLetter(Message message, String errorMsg) {
    if (dlqWriter == null) {
      return false;
    }
    SerializedEvent raw =
        new SerializedEvent(
            message.messageId() == null
                ? null
                : message.messageId().getBytes(StandardCharsets.UTF_8),
            message.body().getBytes(StandardCharsets.UTF_8),
            null);
    dlqWriter.writeToDlq(System.currentTimeMillis(), raw, errorMsg, nodeId);
    return true;
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
