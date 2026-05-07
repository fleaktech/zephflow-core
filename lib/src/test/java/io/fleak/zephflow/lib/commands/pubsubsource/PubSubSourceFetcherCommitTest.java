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
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PubSubSourceFetcherCommitTest {

  private static final String SUBSCRIPTION_PATH = "projects/p/subscriptions/s";

  private SubscriberStub stub;
  private UnaryCallable<PullRequest, PullResponse> pullCallable;
  private UnaryCallable<AcknowledgeRequest, Empty> ackCallable;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    stub = mock(SubscriberStub.class);
    pullCallable = (UnaryCallable<PullRequest, PullResponse>) mock(UnaryCallable.class);
    ackCallable = (UnaryCallable<AcknowledgeRequest, Empty>) mock(UnaryCallable.class);
    when(stub.pullCallable()).thenReturn(pullCallable);
    when(stub.acknowledgeCallable()).thenReturn(ackCallable);
    UnaryCallable<ModifyAckDeadlineRequest, Empty> modAck =
        (UnaryCallable<ModifyAckDeadlineRequest, Empty>) mock(UnaryCallable.class);
    when(stub.modifyAckDeadlineCallable()).thenReturn(modAck);
  }

  private static ReceivedMessage rm(String ackId, String id) {
    return ReceivedMessage.newBuilder()
        .setAckId(ackId)
        .setMessage(
            PubsubMessage.newBuilder()
                .setMessageId(id)
                .setData(ByteString.copyFromUtf8("body"))
                .build())
        .build();
  }

  @Test
  void testCommitAcknowledgesPulledIds() throws Exception {
    PubSubSourceFetcher fetcher = new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);

    PullResponse response =
        PullResponse.newBuilder()
            .addReceivedMessages(rm("a-1", "m-1"))
            .addReceivedMessages(rm("a-2", "m-2"))
            .build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);
    when(ackCallable.call(any(AcknowledgeRequest.class))).thenReturn(Empty.getDefaultInstance());

    fetcher.fetch();
    Fetcher.Committer committer = fetcher.committer();
    committer.commit();

    verify(ackCallable)
        .call(
            argThat(
                (AcknowledgeRequest req) -> {
                  assertEquals(SUBSCRIPTION_PATH, req.getSubscription());
                  assertEquals(2, req.getAckIdsCount());
                  assertTrue(req.getAckIdsList().contains("a-1"));
                  assertTrue(req.getAckIdsList().contains("a-2"));
                  return true;
                }));
  }

  @Test
  void testCommitWithNoMessages() throws Exception {
    PubSubSourceFetcher fetcher = new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);

    when(pullCallable.call(any(PullRequest.class))).thenReturn(PullResponse.getDefaultInstance());

    fetcher.fetch();
    fetcher.committer().commit();

    verify(ackCallable, never()).call(any(AcknowledgeRequest.class));
  }

  @Test
  void testCommitChunksAtOneThousand() throws Exception {
    PubSubSourceFetcher fetcher = new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 1500, false, 0);

    PullResponse.Builder builder = PullResponse.newBuilder();
    for (int i = 0; i < 1500; i++) {
      builder.addReceivedMessages(rm("a-" + i, "m-" + i));
    }
    when(pullCallable.call(any(PullRequest.class))).thenReturn(builder.build());
    when(ackCallable.call(any(AcknowledgeRequest.class))).thenReturn(Empty.getDefaultInstance());

    fetcher.fetch();
    fetcher.committer().commit();

    verify(ackCallable, times(2)).call(any(AcknowledgeRequest.class));
  }

  @Test
  void testCommitHandlesAckFailureGracefully() throws Exception {
    PubSubSourceFetcher fetcher = new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);

    PullResponse response = PullResponse.newBuilder().addReceivedMessages(rm("a-1", "m-1")).build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);
    when(ackCallable.call(any(AcknowledgeRequest.class)))
        .thenThrow(new RuntimeException("ack failed"));

    fetcher.fetch();

    assertDoesNotThrow(() -> fetcher.committer().commit());
    verify(ackCallable).call(any(AcknowledgeRequest.class));
  }
}
