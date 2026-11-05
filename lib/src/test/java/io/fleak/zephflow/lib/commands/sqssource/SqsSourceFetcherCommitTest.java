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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.SourceLoopHarness;
import io.fleak.zephflow.lib.commands.source.SourceLoopHarness.ProcessKilled;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

class SqsSourceFetcherCommitTest {

  private static final String QUEUE_URL =
      "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";

  private SqsClient sqsClient;
  private SqsSourceFetcher fetcher;

  @BeforeEach
  void setUp() {
    sqsClient = mock(SqsClient.class);
    fetcher = new SqsSourceFetcher(sqsClient, QUEUE_URL, 10, 20, 30);
  }

  @Test
  void testCommitDeletesMessages() throws Exception {
    Message message1 =
        Message.builder()
            .body("{\"key\": \"value1\"}")
            .messageId("msg-1")
            .receiptHandle("receipt-1")
            .build();
    Message message2 =
        Message.builder()
            .body("{\"key\": \"value2\"}")
            .messageId("msg-2")
            .receiptHandle("receipt-2")
            .build();

    ReceiveMessageResponse response =
        ReceiveMessageResponse.builder().messages(message1, message2).build();
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

    fetcher.fetch();

    Fetcher.Committer committer = fetcher.committer();
    committer.commit();

    verify(sqsClient)
        .deleteMessage(
            argThat(
                (DeleteMessageRequest req) ->
                    QUEUE_URL.equals(req.queueUrl()) && "receipt-1".equals(req.receiptHandle())));
    verify(sqsClient)
        .deleteMessage(
            argThat(
                (DeleteMessageRequest req) ->
                    QUEUE_URL.equals(req.queueUrl()) && "receipt-2".equals(req.receiptHandle())));
  }

  @Test
  void testCommitWithNoMessages() throws Exception {
    ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(List.of()).build();
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

    fetcher.fetch();

    Fetcher.Committer committer = fetcher.committer();
    committer.commit();

    verify(sqsClient, never()).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  void testCommitHandlesDeleteFailureGracefully() throws Exception {
    Message message1 =
        Message.builder()
            .body("{\"key\": \"value1\"}")
            .messageId("msg-1")
            .receiptHandle("receipt-1")
            .build();
    Message message2 =
        Message.builder()
            .body("{\"key\": \"value2\"}")
            .messageId("msg-2")
            .receiptHandle("receipt-2")
            .build();

    ReceiveMessageResponse response =
        ReceiveMessageResponse.builder().messages(message1, message2).build();
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

    doThrow(new RuntimeException("Delete failed"))
        .doReturn(DeleteMessageResponse.builder().build())
        .when(sqsClient)
        .deleteMessage(any(DeleteMessageRequest.class));

    fetcher.fetch();

    Fetcher.Committer committer = fetcher.committer();
    committer.commit();

    verify(sqsClient, times(2)).deleteMessage(any(DeleteMessageRequest.class));
  }

  @Test
  void commitThroughDeletesOnlyHandledMessages() throws Exception {
    List<String> deleted = new ArrayList<>();
    when(sqsClient.deleteMessage(any(DeleteMessageRequest.class)))
        .thenAnswer(
            call -> {
              deleted.add(call.<DeleteMessageRequest>getArgument(0).receiptHandle());
              return DeleteMessageResponse.builder().build();
            });
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(received("r1", "r2", "r3"))
        .thenReturn(received("r4", "r5"))
        .thenReturn(received());
    Fetcher.Committer committer = fetcher.committer();
    List<List<String>> deletedPerCommit = new ArrayList<>();

    fetcher.fetch();
    committer.commitThrough(1);
    deletedPerCommit.add(drain(deleted));
    fetcher.fetch();
    committer.commitThrough(1);
    deletedPerCommit.add(drain(deleted));
    committer.commitThrough(2);
    deletedPerCommit.add(drain(deleted));
    fetcher.fetch();
    committer.commit();
    deletedPerCommit.add(drain(deleted));

    assertEquals(
        List.of(List.of("r1"), List.of("r2", "r3", "r4"), List.of("r5"), List.of()),
        deletedPerCommit);
  }

  private static ReceiveMessageResponse received(String... receiptHandles) {
    return ReceiveMessageResponse.builder()
        .messages(
            Arrays.stream(receiptHandles)
                .map(
                    handle ->
                        Message.builder()
                            .body("{}")
                            .messageId(handle)
                            .receiptHandle(handle)
                            .build())
                .toList())
        .build();
  }

  private static List<String> drain(List<String> values) {
    List<String> drained = List.copyOf(values);
    values.clear();
    return drained;
  }

  @Test
  void aProcessKilledMidFetchDeletesOnlyTheHandledMessages() {
    List<String> deleted = recordDeletes();
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(received("r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9", "r10"));

    assertThrows(
        ProcessKilled.class,
        () ->
            SourceLoopHarness.run(
                fetcher,
                SourceLoopHarness.oneEventPerRecord(),
                SourceLoopHarness.killedAt(2),
                null));

    assertEquals(List.of("r1"), deleted);
  }

  @Test
  void failedMessagesAfterAHandledOneAreDeletedWhenTheFetchEnds() {
    List<String> deleted = recordDeletes();
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(received("r1", "r2", "r3"));

    SourceLoopHarness.run(
        fetcher,
        SourceLoopHarness.oneEventPerRecord(),
        SourceLoopHarness.failing(record -> record > 1),
        mock(DlqWriter.class));

    assertEquals(List.of("r1", "r2", "r3"), deleted);
  }

  @Test
  void aFetchInWhichEveryMessageFailsIsNotDeleted() {
    List<String> deleted = recordDeletes();
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(received("r1", "r2"));

    SourceLoopHarness.run(
        fetcher,
        SourceLoopHarness.oneEventPerRecord(),
        SourceLoopHarness.failing(record -> true),
        mock(DlqWriter.class));

    assertEquals(List.of(), deleted);
  }

  private List<String> recordDeletes() {
    List<String> deleted = new ArrayList<>();
    when(sqsClient.deleteMessage(any(DeleteMessageRequest.class)))
        .thenAnswer(
            call -> {
              deleted.add(call.<DeleteMessageRequest>getArgument(0).receiptHandle());
              return DeleteMessageResponse.builder().build();
            });
    return deleted;
  }
}
