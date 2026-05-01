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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PubSubSourceFetcherTest {

  private static final String SUBSCRIPTION_PATH = "projects/p/subscriptions/s";

  private SubscriberStub stub;
  private UnaryCallable<PullRequest, PullResponse> pullCallable;
  private UnaryCallable<ModifyAckDeadlineRequest, Empty> modAckCallable;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    stub = mock(SubscriberStub.class);
    pullCallable = (UnaryCallable<PullRequest, PullResponse>) mock(UnaryCallable.class);
    modAckCallable = (UnaryCallable<ModifyAckDeadlineRequest, Empty>) mock(UnaryCallable.class);
    when(stub.pullCallable()).thenReturn(pullCallable);
    when(stub.modifyAckDeadlineCallable()).thenReturn(modAckCallable);
  }

  private static ReceivedMessage receivedMessage(
      String ackId, String messageId, String body, Map<String, String> attrs, String orderingKey) {
    PubsubMessage.Builder mb =
        PubsubMessage.newBuilder()
            .setMessageId(messageId)
            .setData(ByteString.copyFromUtf8(body))
            .putAllAttributes(attrs);
    if (orderingKey != null) {
      mb.setOrderingKey(orderingKey);
    }
    return ReceivedMessage.newBuilder().setAckId(ackId).setMessage(mb.build()).build();
  }

  @Test
  void testFetchReturnsMessages() {
    PubSubSourceFetcher fetcher = new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 100, false, 0);

    PullResponse response =
        PullResponse.newBuilder()
            .addReceivedMessages(receivedMessage("a-1", "m-1", "{\"k\":1}", Map.of(), null))
            .addReceivedMessages(receivedMessage("a-2", "m-2", "{\"k\":2}", Map.of(), null))
            .build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);

    List<PubSubReceivedMessage> result = fetcher.fetch();

    assertEquals(2, result.size());
    assertEquals("m-1", result.get(0).messageId());
    assertEquals("a-1", result.get(0).ackId());
    assertArrayEquals("{\"k\":1}".getBytes(), result.get(0).body());
    assertEquals("m-1", result.get(0).attributes().get("messageId"));

    verify(pullCallable)
        .call(
            argThat(
                (PullRequest req) -> {
                  assertEquals(SUBSCRIPTION_PATH, req.getSubscription());
                  assertEquals(100, req.getMaxMessages());
                  assertFalse(req.getReturnImmediately());
                  return true;
                }));
    verify(modAckCallable, never()).call(any());
  }

  @Test
  void testFetchPropagatesAttributes() {
    PubSubSourceFetcher fetcher = new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);

    PullResponse response =
        PullResponse.newBuilder()
            .addReceivedMessages(
                receivedMessage("a-1", "m-1", "body", Map.of("attr1", "v1"), "ord-1"))
            .build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);

    List<PubSubReceivedMessage> result = fetcher.fetch();

    assertEquals(1, result.size());
    Map<String, String> attrs = result.get(0).attributes();
    assertEquals("v1", attrs.get("attr1"));
    assertEquals("m-1", attrs.get("messageId"));
    assertEquals("ord-1", attrs.get("orderingKey"));
  }

  @Test
  void testFetchReturnsEmptyWhenNoMessages() {
    PubSubSourceFetcher fetcher = new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);

    when(pullCallable.call(any(PullRequest.class))).thenReturn(PullResponse.getDefaultInstance());

    List<PubSubReceivedMessage> result = fetcher.fetch();

    assertTrue(result.isEmpty());
    verify(modAckCallable, never()).call(any());
  }

  @Test
  void testFetchExtendsAckDeadlineWhenConfigured() {
    PubSubSourceFetcher fetcher = new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 120);

    PullResponse response =
        PullResponse.newBuilder()
            .addReceivedMessages(receivedMessage("a-1", "m-1", "body", Map.of(), null))
            .addReceivedMessages(receivedMessage("a-2", "m-2", "body", Map.of(), null))
            .build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);
    when(modAckCallable.call(any(ModifyAckDeadlineRequest.class)))
        .thenReturn(Empty.getDefaultInstance());

    fetcher.fetch();

    verify(modAckCallable)
        .call(
            argThat(
                (ModifyAckDeadlineRequest req) -> {
                  assertEquals(SUBSCRIPTION_PATH, req.getSubscription());
                  assertEquals(120, req.getAckDeadlineSeconds());
                  assertEquals(List.of("a-1", "a-2"), req.getAckIdsList());
                  return true;
                }));
  }

  @Test
  void testFetchSwallowsModifyAckDeadlineFailure() {
    PubSubSourceFetcher fetcher = new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 60);

    PullResponse response =
        PullResponse.newBuilder()
            .addReceivedMessages(receivedMessage("a-1", "m-1", "body", Map.of(), null))
            .build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);
    when(modAckCallable.call(any(ModifyAckDeadlineRequest.class)))
        .thenThrow(new RuntimeException("transient"));

    List<PubSubReceivedMessage> result = fetcher.fetch();

    assertEquals(1, result.size());
  }

  @Test
  void testIsExhaustedReturnsFalse() {
    PubSubSourceFetcher fetcher = new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);
    assertFalse(fetcher.isExhausted());
  }

  @Test
  void testCommitStrategyIsPerRecord() {
    PubSubSourceFetcher fetcher = new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);
    assertEquals(
        io.fleak.zephflow.lib.commands.source.PerRecordCommitStrategy.INSTANCE,
        fetcher.commitStrategy());
  }
}
