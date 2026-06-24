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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

class S3RealtimeSourceFetcherTest {

  private static final String QUEUE_URL =
      "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
  private static final int MAX_RETRIES = 3;

  private SqsClient sqsClient;
  private S3Client s3Client;
  private DlqWriter dlqWriter;
  private Queue<String> confirmed;
  private S3RealtimeSourceFetcher fetcher;

  @BeforeEach
  void setUp() {
    sqsClient = mock(SqsClient.class);
    s3Client = mock(S3Client.class);
    dlqWriter = mock(DlqWriter.class);
    confirmed = new ConcurrentLinkedQueue<>();
    fetcher = newFetcher(dlqWriter);
  }

  private S3RealtimeSourceFetcher newFetcher(DlqWriter dlq) {
    return new S3RealtimeSourceFetcher(
        sqsClient, s3Client, QUEUE_URL, 10, 20, 30, MAX_RETRIES, dlq, "nodeId", confirmed);
  }

  private static Message message(String id, String receipt, int receiveCount, String body) {
    return Message.builder()
        .messageId(id)
        .receiptHandle(receipt)
        .body(body)
        .attributes(
            Map.of(
                MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, String.valueOf(receiveCount)))
        .build();
  }

  private static String objectCreated(String bucket, String key) {
    return "{\"Records\":[{\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"bucket\":{\"name\":\""
        + bucket
        + "\"},\"object\":{\"key\":\""
        + key
        + "\"}}}]}";
  }

  private void stubReceive(Message... messages) {
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(ReceiveMessageResponse.builder().messages(messages).build());
  }

  @Test
  void fetch_validObjectCreatedMessage() {
    stubReceive(message("msg-1", "receipt-1", 1, objectCreated("b", "k.json")));

    List<S3EventMessage> result = fetcher.fetch();

    assertEquals(
        List.of(
            new S3EventMessage(
                "msg-1",
                "receipt-1",
                objectCreated("b", "k.json"),
                List.of(new S3ObjectRef("b", "k.json")))),
        result);
    verify(sqsClient, never()).deleteMessage(any(DeleteMessageRequest.class));
    verifyNoInteractions(dlqWriter);
  }

  @Test
  void fetch_testEventDeletedAndNotEmitted() {
    stubReceive(
        message("msg-1", "receipt-1", 1, "{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\"}"));

    List<S3EventMessage> result = fetcher.fetch();

    assertTrue(result.isEmpty());
    verify(sqsClient).deleteMessage(deleteFor("receipt-1"));
    verifyNoInteractions(dlqWriter);
  }

  @Test
  void fetch_exceededMaxRetriesDeadLetteredAndDeleted() {
    stubReceive(message("msg-1", "receipt-1", MAX_RETRIES + 1, objectCreated("b", "k.json")));

    List<S3EventMessage> result = fetcher.fetch();

    assertTrue(result.isEmpty());
    verify(dlqWriter).writeToDlq(anyLong(), any(), contains("exceeded maxRetries"), eq("nodeId"));
    verify(sqsClient).deleteMessage(deleteFor("receipt-1"));
  }

  @Test
  void fetch_exceededMaxRetriesNoDlqStillDeleted() {
    fetcher = newFetcher(null);
    stubReceive(message("msg-1", "receipt-1", MAX_RETRIES + 1, objectCreated("b", "k.json")));

    List<S3EventMessage> result = fetcher.fetch();

    assertTrue(result.isEmpty());
    verify(sqsClient).deleteMessage(deleteFor("receipt-1"));
  }

  @Test
  void fetch_malformedMessageDeadLetteredAndDeleted() {
    stubReceive(message("msg-1", "receipt-1", 1, "not-json"));

    List<S3EventMessage> result = fetcher.fetch();

    assertTrue(result.isEmpty());
    verify(dlqWriter).writeToDlq(anyLong(), any(), contains("failed to parse"), eq("nodeId"));
    verify(sqsClient).deleteMessage(deleteFor("receipt-1"));
  }

  @Test
  void committer_deletesOnlyConfirmedHandles() throws Exception {
    confirmed.add("receipt-confirmed");

    fetcher.committer().commit();

    verify(sqsClient).deleteMessage(deleteFor("receipt-confirmed"));
    verify(sqsClient, never()).deleteMessage(deleteFor("receipt-unprocessed"));
    assertTrue(confirmed.isEmpty());
  }

  @Test
  void isExhausted_alwaysFalse() {
    assertFalse(fetcher.isExhausted());
  }

  @Test
  void close_closesClients() throws Exception {
    fetcher.close();
    verify(sqsClient).close();
    verify(s3Client).close();
    // The DLQ writer is owned/closed by the execution context, not the fetcher.
    verify(dlqWriter, never()).close();
  }

  private static DeleteMessageRequest deleteFor(String receiptHandle) {
    return argThat(
        (DeleteMessageRequest r) ->
            r != null && QUEUE_URL.equals(r.queueUrl()) && receiptHandle.equals(r.receiptHandle()));
  }
}
