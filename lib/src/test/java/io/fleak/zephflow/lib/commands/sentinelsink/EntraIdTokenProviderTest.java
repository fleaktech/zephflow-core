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
package io.fleak.zephflow.lib.commands.sentinelsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EntraIdTokenProviderTest {

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private EntraIdTokenProvider provider;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    provider = new EntraIdTokenProvider("tenant-id", "client-id", "client-secret", mockHttpClient);
  }

  @Test
  void getToken_fetchesAndCachesToken() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"access_token\":\"my-token\",\"expires_in\":3600}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    String token = provider.getToken();

    assertEquals("my-token", token);
    verify(mockHttpClient, times(1)).send(any(), any());
  }

  @Test
  void getToken_returnsCachedTokenOnSecondCall() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"access_token\":\"my-token\",\"expires_in\":3600}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.getToken();
    provider.getToken();

    verify(mockHttpClient, times(1)).send(any(), any());
  }

  @Test
  void invalidate_clearsCache_forcesNewFetch() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"access_token\":\"my-token\",\"expires_in\":3600}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.getToken();
    provider.invalidate();
    provider.getToken();

    verify(mockHttpClient, times(2)).send(any(), any());
  }

  @Test
  void getToken_throwsOnNon200() throws Exception {
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("{\"error\":\"invalid_client\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex = assertThrows(RuntimeException.class, () -> provider.getToken());
    assertTrue(ex.getMessage().contains("401"));
  }

  @Test
  void getToken_throwsWhenAccessTokenMissingFromResponse() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"token_type\":\"Bearer\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex = assertThrows(RuntimeException.class, () -> provider.getToken());
    assertTrue(ex.getMessage().contains("access_token"));
  }
}
