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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

class SqsSourceFetcherTest {

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
  void testFetchReturnsMessages() {
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

    List<SqsReceivedMessage> result = fetcher.fetch();

    assertEquals(2, result.size());
    assertEquals("msg-1", result.get(0).messageId());
    assertEquals("receipt-1", result.get(0).receiptHandle());
    assertArrayEquals("{\"key\": \"value1\"}".getBytes(), result.get(0).body());
    assertEquals("msg-2", result.get(1).messageId());

    verify(sqsClient)
        .receiveMessage(
            argThat(
                (ReceiveMessageRequest req) -> {
                  assertEquals(QUEUE_URL, req.queueUrl());
                  assertEquals(10, req.maxNumberOfMessages());
                  assertEquals(20, req.waitTimeSeconds());
                  assertEquals(30, req.visibilityTimeout());
                  return true;
                }));
  }

  @Test
  void testFetchWithMessageAttributes() {
    Message message =
        Message.builder()
            .body("{\"data\": \"test\"}")
            .messageId("msg-1")
            .receiptHandle("receipt-1")
            .messageAttributes(
                java.util.Map.of(
                    "customAttr",
                    MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue("customValue")
                        .build()))
            .build();

    ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(message).build();
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

    List<SqsReceivedMessage> result = fetcher.fetch();

    assertEquals(1, result.size());
    assertEquals("customValue", result.get(0).attributes().get("customAttr"));
  }

  @Test
  void testFetchReturnsEmptyWhenNoMessages() {
    ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(List.of()).build();
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

    List<SqsReceivedMessage> result = fetcher.fetch();

    assertTrue(result.isEmpty());
  }

  @Test
  void testIsExhaustedReturnsFalse() {
    assertFalse(fetcher.isExhausted());
  }

  @Test
  void testCommitStrategyIsPerRecord() {
    assertEquals(
        io.fleak.zephflow.lib.commands.source.PerRecordCommitStrategy.INSTANCE,
        fetcher.commitStrategy());
  }
}
