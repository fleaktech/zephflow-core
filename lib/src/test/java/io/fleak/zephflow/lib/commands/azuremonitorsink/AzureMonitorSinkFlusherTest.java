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
package io.fleak.zephflow.lib.commands.azuremonitorsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AzureMonitorSinkFlusherTest {

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private EntraIdTokenProvider mockTokenProvider;
  private AzureMonitorSinkFlusher flusher;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    mockTokenProvider = mock(EntraIdTokenProvider.class);
    flusher =
        new AzureMonitorSinkFlusher(
            "https://my-dce.eastus.ingest.monitor.azure.com",
            "dcr-abc123",
            "Custom-ZephflowTest_CL",
            mockTokenProvider,
            mockHttpClient);
  }

  @Test
  void flush_emptyBatch_returnsZeroWithNoHttpCall() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<AzureMonitorSinkOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verifyNoInteractions(mockHttpClient);
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_204Response_countsSuccess() throws Exception {
    when(mockTokenProvider.getToken()).thenReturn("test-token");
    when(mockResponse.statusCode()).thenReturn(204);
    when(mockResponse.body()).thenReturn("");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<AzureMonitorSinkOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new AzureMonitorSinkOutboundEvent("{\"msg\":\"hello\"}"));

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(1, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verify(mockHttpClient, times(1)).send(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_401_invalidatesTokenAndRetries_thenSucceeds() throws Exception {
    HttpResponse<String> unauthorizedResponse = mock(HttpResponse.class);
    when(unauthorizedResponse.statusCode()).thenReturn(401);
    when(unauthorizedResponse.body()).thenReturn("Unauthorized");

    when(mockTokenProvider.getToken()).thenReturn("stale-token").thenReturn("fresh-token");
    when(mockResponse.statusCode()).thenReturn(204);
    when(mockResponse.body()).thenReturn("");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(unauthorizedResponse)
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<AzureMonitorSinkOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new AzureMonitorSinkOutboundEvent("{\"msg\":\"hello\"}"));

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(1, result.successCount());
    verify(mockTokenProvider).invalidate();
    verify(mockHttpClient, times(2)).send(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_401AfterRetry_returnsErrorPerEvent() throws Exception {
    when(mockTokenProvider.getToken()).thenReturn("stale-token").thenReturn("fresh-token");
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("Unauthorized");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<AzureMonitorSinkOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new AzureMonitorSinkOutboundEvent("{\"msg\":\"hello\"}"));

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("401"));
    verify(mockTokenProvider).invalidate();
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_503Response_returnsErrorPerEvent() throws Exception {
    when(mockTokenProvider.getToken()).thenReturn("test-token");
    when(mockResponse.statusCode()).thenReturn(503);
    when(mockResponse.body()).thenReturn("Service Unavailable");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<AzureMonitorSinkOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new AzureMonitorSinkOutboundEvent("{\"msg\":\"hello\"}"));

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("503"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_networkException_returnsErrorPerEvent() throws Exception {
    when(mockTokenProvider.getToken()).thenReturn("test-token");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new RuntimeException("Connection refused"));

    SimpleSinkCommand.PreparedInputEvents<AzureMonitorSinkOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new AzureMonitorSinkOutboundEvent("{\"msg\":\"hello\"}"));

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("Connection refused"));
  }
}
