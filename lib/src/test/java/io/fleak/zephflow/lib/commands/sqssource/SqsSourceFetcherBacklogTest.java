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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.commands.source.SourceBacklog;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

class SqsSourceFetcherBacklogTest {

  private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/1/q";

  private final AtomicLong nanos = new AtomicLong();
  private SqsClient sqsClient;
  private SqsSourceFetcher fetcher;

  @BeforeEach
  void setUp() {
    sqsClient = mock(SqsClient.class);
    when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
        .thenReturn(
            ReceiveMessageResponse.builder()
                .messages(Message.builder().body("{}").messageId("m").receiptHandle("r").build())
                .build());
    fetcher = new SqsSourceFetcher(sqsClient, QUEUE_URL, 10, 20, 30, nanos::get);
  }

  @Test
  void reportsApproximateNumberOfMessages() {
    queueDepth("42");

    fetcher.fetch();

    assertEquals(SourceBacklog.records(42L, null), fetcher.backlog());
  }

  @Test
  void readsTheQueueDepthAtMostEveryThirtySeconds() {
    queueDepth("42");

    fetcher.fetch();
    nanos.addAndGet(Duration.ofSeconds(29).toNanos());
    fetcher.fetch();
    verify(sqsClient, times(1)).getQueueAttributes(any(GetQueueAttributesRequest.class));

    queueDepth("7");
    nanos.addAndGet(Duration.ofSeconds(1).toNanos());
    fetcher.fetch();

    verify(sqsClient, times(2)).getQueueAttributes(any(GetQueueAttributesRequest.class));
    assertEquals(SourceBacklog.records(7L, null), fetcher.backlog());
  }

  @Test
  void aDeniedQueueAttributesReadIsUnknownAndConsumingContinues() {
    when(sqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenThrow(SqsException.builder().statusCode(403).message("AccessDenied").build());

    assertEquals(1, fetcher.fetch().size());

    assertEquals(SourceBacklog.UNKNOWN, fetcher.backlog());
  }

  private void queueDepth(String depth) {
    when(sqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class)))
        .thenReturn(
            GetQueueAttributesResponse.builder()
                .attributes(Map.of(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, depth))
                .build());
  }
}
