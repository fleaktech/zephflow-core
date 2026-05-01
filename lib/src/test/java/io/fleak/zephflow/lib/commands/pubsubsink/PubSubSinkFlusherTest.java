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
package io.fleak.zephflow.lib.commands.pubsubsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PubSubSinkFlusherTest {

  private static final String TOPIC_PATH = "projects/p/topics/t";

  private PublisherStub stub;
  private UnaryCallable<PublishRequest, PublishResponse> publishCallable;
  private PubSubSinkFlusher flusher;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    stub = mock(PublisherStub.class);
    publishCallable = (UnaryCallable<PublishRequest, PublishResponse>) mock(UnaryCallable.class);
    when(stub.publishCallable()).thenReturn(publishCallable);
    flusher = new PubSubSinkFlusher(stub, TOPIC_PATH);
  }

  @Test
  void testSuccessfulFlush() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData r1 = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v1")));
    RecordFleakData r2 = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v2")));
    prepared.add(r1, new PubSubOutboundMessage("{\"d\":\"v1\"}", null));
    prepared.add(r2, new PubSubOutboundMessage("{\"d\":\"v2\"}", null));

    when(publishCallable.call(any(PublishRequest.class)))
        .thenReturn(PublishResponse.newBuilder().addMessageIds("m1").addMessageIds("m2").build());

    SimpleSinkCommand.FlushResult result = flusher.flush(prepared, Map.of());

    assertEquals(2, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    assertTrue(result.flushedDataSize() > 0);

    verify(publishCallable)
        .call(
            argThat(
                (PublishRequest req) -> {
                  assertEquals(TOPIC_PATH, req.getTopic());
                  assertEquals(2, req.getMessagesCount());
                  assertEquals("{\"d\":\"v1\"}", req.getMessages(0).getData().toStringUtf8());
                  assertEquals("{\"d\":\"v2\"}", req.getMessages(1).getData().toStringUtf8());
                  return true;
                }));
  }

  @Test
  void testFlushWithOrderingKey() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData r = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v")));
    prepared.add(r, new PubSubOutboundMessage("{\"d\":\"v\"}", "tenant-1"));

    when(publishCallable.call(any(PublishRequest.class)))
        .thenReturn(PublishResponse.newBuilder().addMessageIds("m1").build());

    SimpleSinkCommand.FlushResult result = flusher.flush(prepared, Map.of());

    assertEquals(1, result.successCount());

    verify(publishCallable)
        .call(
            argThat(
                (PublishRequest req) -> {
                  assertEquals("tenant-1", req.getMessages(0).getOrderingKey());
                  return true;
                }));
  }

  @Test
  void testFlushWithoutOrderingKey() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData r = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v")));
    prepared.add(r, new PubSubOutboundMessage("{\"d\":\"v\"}", null));

    when(publishCallable.call(any(PublishRequest.class)))
        .thenReturn(PublishResponse.newBuilder().addMessageIds("m1").build());

    flusher.flush(prepared, Map.of());

    verify(publishCallable)
        .call(
            argThat(
                (PublishRequest req) -> {
                  assertEquals("", req.getMessages(0).getOrderingKey());
                  return true;
                }));
  }

  @Test
  void testEmptyBatch() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(prepared, Map.of());

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verify(publishCallable, never()).call(any(PublishRequest.class));
  }

  @Test
  void testPublisherException() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData r1 = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v1")));
    RecordFleakData r2 = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v2")));
    prepared.add(r1, new PubSubOutboundMessage("{\"d\":\"v1\"}", null));
    prepared.add(r2, new PubSubOutboundMessage("{\"d\":\"v2\"}", null));

    when(publishCallable.call(any(PublishRequest.class)))
        .thenThrow(new RuntimeException("Connection timeout"));

    SimpleSinkCommand.FlushResult result = flusher.flush(prepared, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(2, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("Pub/Sub publish failed"));
  }
}
