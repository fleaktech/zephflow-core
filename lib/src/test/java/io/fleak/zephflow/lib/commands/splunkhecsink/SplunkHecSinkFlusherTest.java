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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SplunkHecSinkFlusherTest {

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private SplunkHecSinkFlusher flusher;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    flusher =
        new SplunkHecSinkFlusher(
            "https://splunk:8088/services/collector/event", "abcd-1234", mockHttpClient);
  }

  private SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> oneEvent(String line) {
    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new SplunkHecOutboundEvent(line.getBytes(StandardCharsets.UTF_8)));
    return events;
  }

  @Test
  void flush_emptyBatch_noHttpCall() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verifyNoInteractions(mockHttpClient);
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_200_returnsSuccessForAllRecords() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"text\":\"Success\",\"code\":0}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        oneEvent("{\"event\":{\"msg\":\"hello\"}}\n");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(1, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    assertTrue(result.flushedDataSize() > 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_4xxWithHecErrorJson_returnsErrorPerRecordWithHecReason() throws Exception {
    when(mockResponse.statusCode()).thenReturn(403);
    when(mockResponse.body()).thenReturn("{\"text\":\"Invalid token\",\"code\":4}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        oneEvent("{\"event\":{\"msg\":\"hello\"}}\n");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    String reason = result.errorOutputList().get(0).errorMessage();
    assertTrue(reason.contains("403"), "reason should include status code: " + reason);
    assertTrue(reason.contains("Invalid token"), "reason should include HEC text: " + reason);
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_5xxWithNonJsonBody_returnsErrorPerRecordWithStatusFallback() throws Exception {
    when(mockResponse.statusCode()).thenReturn(503);
    when(mockResponse.body()).thenReturn("Service Unavailable");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        oneEvent("{\"event\":{\"msg\":\"hello\"}}\n");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    String reason = result.errorOutputList().get(0).errorMessage();
    assertTrue(reason.contains("503"), "reason should include status code: " + reason);
    assertTrue(
        reason.contains("Service Unavailable"),
        "reason should include raw body when not HEC JSON: " + reason);
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_401_returnsAuthErrorPerRecord_noRetry() throws Exception {
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("{\"text\":\"Unauthorized\",\"code\":2}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        oneEvent("{\"event\":{\"msg\":\"hello\"}}\n");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("401"));
    verify(mockHttpClient, times(1)).send(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_networkException_propagates() throws Exception {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new java.io.IOException("Connection refused"));

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        oneEvent("{\"event\":{\"msg\":\"hello\"}}\n");

    Exception thrown = assertThrows(Exception.class, () -> flusher.flush(events, Map.of()));
    assertTrue(thrown.getMessage().contains("Connection refused"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_buildsValidNdjsonBodyWithSplunkAuthHeader() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"text\":\"Success\",\"code\":0}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData rec1 = new RecordFleakData(Map.of("a", new StringPrimitiveFleakData("1")));
    RecordFleakData rec2 = new RecordFleakData(Map.of("b", new StringPrimitiveFleakData("2")));
    events.add(rec1, new SplunkHecOutboundEvent("{\"event\":{\"a\":\"1\"}}\n".getBytes()));
    events.add(rec2, new SplunkHecOutboundEvent("{\"event\":{\"b\":\"2\"}}\n".getBytes()));

    flusher.flush(events, Map.of());

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockHttpClient).send(captor.capture(), any());
    HttpRequest sent = captor.getValue();
    assertEquals(
        "Splunk abcd-1234",
        sent.headers().firstValue("Authorization").orElseThrow(),
        "Auth header must use 'Splunk <token>' scheme");
    assertEquals("application/json", sent.headers().firstValue("Content-Type").orElseThrow());
    assertEquals("https://splunk:8088/services/collector/event", sent.uri().toString());
  }
}
