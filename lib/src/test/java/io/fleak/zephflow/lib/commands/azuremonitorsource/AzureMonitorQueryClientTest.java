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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AzureMonitorQueryClientTest {

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private EntraIdTokenProvider mockTokenProvider;
  private AzureMonitorQueryClient client;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    mockTokenProvider = mock(EntraIdTokenProvider.class);
    when(mockTokenProvider.getToken()).thenReturn("test-token");
    client = new AzureMonitorQueryClient("workspace-id", mockTokenProvider, mockHttpClient);
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_parsesColumnsAndRows() throws Exception {
    String responseBody =
        """
        {
          "tables": [{
            "columns": [
              {"name": "TimeGenerated", "type": "datetime"},
              {"name": "message", "type": "string"},
              {"name": "severity", "type": "string"}
            ],
            "rows": [
              ["2024-01-01T00:00:00Z", "hello", "info"],
              ["2024-01-01T01:00:00Z", "world", "warn"]
            ]
          }]
        }
        """;
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseBody);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    List<Map<String, String>> result = client.executeQuery("TableName_CL | limit 10");

    assertEquals(2, result.size());
    assertEquals("2024-01-01T00:00:00Z", result.get(0).get("TimeGenerated"));
    assertEquals("hello", result.get(0).get("message"));
    assertEquals("info", result.get(0).get("severity"));
    assertEquals("world", result.get(1).get("message"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_nullValueBecomesEmptyString() throws Exception {
    String responseBody =
        """
        {
          "tables": [{
            "columns": [{"name": "col1", "type": "string"}],
            "rows": [[null]]
          }]
        }
        """;
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseBody);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    List<Map<String, String>> result = client.executeQuery("T | limit 1");

    assertEquals(1, result.size());
    assertEquals("", result.get(0).get("col1"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_401_retriesOnce_succeeds() throws Exception {
    HttpResponse<String> unauthorizedResponse = mock(HttpResponse.class);
    when(unauthorizedResponse.statusCode()).thenReturn(401);
    when(unauthorizedResponse.body()).thenReturn("Unauthorized");

    when(mockTokenProvider.getToken()).thenReturn("stale-token").thenReturn("fresh-token");
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body())
        .thenReturn(
            "{\"tables\":[{\"columns\":[{\"name\":\"c\",\"type\":\"string\"}],\"rows\":[[\"v\"]]}]}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(unauthorizedResponse)
        .thenReturn(mockResponse);

    List<Map<String, String>> result = client.executeQuery("T");

    assertEquals(1, result.size());
    assertEquals("v", result.get(0).get("c"));
    verify(mockTokenProvider).invalidate();
    verify(mockHttpClient, times(2)).send(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_401_afterRetry_throwsRuntimeException() throws Exception {
    when(mockTokenProvider.getToken()).thenReturn("stale-token").thenReturn("fresh-token");
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("Unauthorized");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex = assertThrows(RuntimeException.class, () -> client.executeQuery("T"));
    assertTrue(ex.getMessage().contains("token rejected after refresh"));
    verify(mockTokenProvider).invalidate();
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_non200_throwsWithStatusAndBody() throws Exception {
    when(mockResponse.statusCode()).thenReturn(500);
    when(mockResponse.body()).thenReturn("Internal Server Error");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex = assertThrows(RuntimeException.class, () -> client.executeQuery("T"));
    assertTrue(ex.getMessage().contains("500"));
    assertTrue(ex.getMessage().contains("Internal Server Error"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_emptyTablesArray_throwsRuntimeException() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"tables\":[]}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex = assertThrows(RuntimeException.class, () -> client.executeQuery("T"));
    assertTrue(ex.getMessage().toLowerCase().contains("tables"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_missingTablesField_throwsRuntimeException() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"someOtherField\":42}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex = assertThrows(RuntimeException.class, () -> client.executeQuery("T"));
    assertTrue(ex.getMessage().toLowerCase().contains("tables"));
  }
}
