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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

class SqsSinkFlusherTest {

  private static final String QUEUE_URL =
      "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";

  private SqsClient sqsClient;
  private SqsSinkFlusher flusher;

  @BeforeEach
  void setUp() {
    sqsClient = mock(SqsClient.class);
    flusher = new SqsSinkFlusher(sqsClient, QUEUE_URL);
  }

  @Test
  void testSuccessfulFlush() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<SqsOutboundMessage> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData record1 =
        new RecordFleakData(Map.of("data", new StringPrimitiveFleakData("value1")));
    RecordFleakData record2 =
        new RecordFleakData(Map.of("data", new StringPrimitiveFleakData("value2")));

    preparedInputEvents.add(record1, new SqsOutboundMessage("{\"data\":\"value1\"}", null, null));
    preparedInputEvents.add(record2, new SqsOutboundMessage("{\"data\":\"value2\"}", null, null));

    SendMessageBatchResponse response =
        SendMessageBatchResponse.builder()
            .successful(
                SendMessageBatchResultEntry.builder().id("0").messageId("m1").build(),
                SendMessageBatchResultEntry.builder().id("1").messageId("m2").build())
            .failed(List.of())
            .build();

    when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(response);

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents, Map.of());

    assertEquals(2, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    assertTrue(result.flushedDataSize() > 0);

    verify(sqsClient)
        .sendMessageBatch(
            argThat(
                (SendMessageBatchRequest req) -> {
                  assertEquals(QUEUE_URL, req.queueUrl());
                  assertEquals(2, req.entries().size());
                  assertEquals("{\"data\":\"value1\"}", req.entries().get(0).messageBody());
                  assertEquals("{\"data\":\"value2\"}", req.entries().get(1).messageBody());
                  return true;
                }));
  }

  @Test
  void testFlushWithFifoAttributes() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<SqsOutboundMessage> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData record =
        new RecordFleakData(Map.of("data", new StringPrimitiveFleakData("value1")));
    preparedInputEvents.add(
        record, new SqsOutboundMessage("{\"data\":\"value1\"}", "group-1", "dedup-1"));

    SendMessageBatchResponse response =
        SendMessageBatchResponse.builder()
            .successful(SendMessageBatchResultEntry.builder().id("0").messageId("m1").build())
            .failed(List.of())
            .build();

    when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(response);

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents, Map.of());

    assertEquals(1, result.successCount());

    verify(sqsClient)
        .sendMessageBatch(
            argThat(
                (SendMessageBatchRequest req) -> {
                  SendMessageBatchRequestEntry entry = req.entries().get(0);
                  assertEquals("group-1", entry.messageGroupId());
                  assertEquals("dedup-1", entry.messageDeduplicationId());
                  return true;
                }));
  }

  @Test
  void testPartialFailure() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<SqsOutboundMessage> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData record1 =
        new RecordFleakData(Map.of("data", new StringPrimitiveFleakData("value1")));
    RecordFleakData record2 =
        new RecordFleakData(Map.of("data", new StringPrimitiveFleakData("value2")));

    preparedInputEvents.add(record1, new SqsOutboundMessage("{\"data\":\"value1\"}", null, null));
    preparedInputEvents.add(record2, new SqsOutboundMessage("{\"data\":\"value2\"}", null, null));

    SendMessageBatchResponse response =
        SendMessageBatchResponse.builder()
            .successful(SendMessageBatchResultEntry.builder().id("0").messageId("m1").build())
            .failed(
                BatchResultErrorEntry.builder()
                    .id("1")
                    .code("InternalError")
                    .message("Internal error occurred")
                    .build())
            .build();

    when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class))).thenReturn(response);

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents, Map.of());

    assertEquals(1, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("InternalError"));
  }

  @Test
  void testEmptyBatch() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<SqsOutboundMessage> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents, Map.of());

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verify(sqsClient, never()).sendMessageBatch(any(SendMessageBatchRequest.class));
  }

  @Test
  void testSqsClientException() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<SqsOutboundMessage> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData record =
        new RecordFleakData(Map.of("data", new StringPrimitiveFleakData("value1")));
    preparedInputEvents.add(record, new SqsOutboundMessage("{\"data\":\"value1\"}", null, null));

    when(sqsClient.sendMessageBatch(any(SendMessageBatchRequest.class)))
        .thenThrow(new RuntimeException("Connection timeout"));

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("SQS client error"));
  }
}
