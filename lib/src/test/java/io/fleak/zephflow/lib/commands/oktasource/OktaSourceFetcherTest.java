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
package io.fleak.zephflow.lib.commands.oktasource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class OktaSourceFetcherTest {

  private static HttpResponse<String> mockResponse(int status, String body, String linkHeader) {
    @SuppressWarnings("unchecked")
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(status);
    when(response.body()).thenReturn(body);
    HttpHeaders headers =
        linkHeader == null
            ? HttpHeaders.of(Map.of(), (k, v) -> true)
            : HttpHeaders.of(Map.of("Link", List.of(linkHeader)), (k, v) -> true);
    when(response.headers()).thenReturn(headers);
    return response;
  }

  @Test
  void fetchParsesEventsFromSuccessResponse() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    String body =
        "[{\"uuid\":\"evt-1\",\"eventType\":\"user.session.start\"},"
            + "{\"uuid\":\"evt-2\",\"eventType\":\"user.session.end\"}]";
    doReturn(mockResponse(200, body, null)).when(httpClient).send(any(HttpRequest.class), any());

    OktaSourceFetcher fetcher =
        new OktaSourceFetcher(
            "mycompany.okta.com", "tok", "2026-01-01T00:00:00Z", null, 100, httpClient);

    List<OktaLogEvent> events = fetcher.fetch();

    assertEquals(2, events.size());
    assertEquals("evt-1", events.get(0).eventId());
    assertEquals("user.session.start", events.get(0).payload().get("eventType"));
    assertEquals("evt-2", events.get(1).eventId());
  }

  @Test
  void fetchSendsAuthorizationHeaderAndCorrectUrl() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    doReturn(mockResponse(200, "[]", null)).when(httpClient).send(any(HttpRequest.class), any());

    OktaSourceFetcher fetcher =
        new OktaSourceFetcher(
            "mycompany.okta.com",
            "tok",
            "2026-01-01T00:00:00Z",
            "eventType eq \"user.session.start\"",
            50,
            httpClient);

    fetcher.fetch();

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient).send(captor.capture(), any());
    HttpRequest req = captor.getValue();
    assertEquals(Optional.of("SSWS tok"), req.headers().firstValue("Authorization"));
    String url = req.uri().toString();
    assertTrue(url.startsWith("https://mycompany.okta.com/api/v1/logs"));
    assertTrue(url.contains("limit=50"));
    assertTrue(url.contains("since=2026-01-01T00%3A00%3A00Z"));
    assertTrue(url.contains("filter=eventType+eq+%22user.session.start%22"));
  }

  @Test
  void fetchUsesNextUrlOnSubsequentCallWhenLinkHeaderPresent() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    String firstBody = "[{\"uuid\":\"evt-1\"}]";
    String linkHeader = "<https://mycompany.okta.com/api/v1/logs?after=cursor-1>; rel=\"next\"";
    String secondBody = "[{\"uuid\":\"evt-2\"}]";

    doReturn(mockResponse(200, firstBody, linkHeader), mockResponse(200, secondBody, null))
        .when(httpClient)
        .send(any(HttpRequest.class), any());

    OktaSourceFetcher fetcher =
        new OktaSourceFetcher(
            "mycompany.okta.com", "tok", "2026-01-01T00:00:00Z", null, 100, httpClient);

    fetcher.fetch();
    fetcher.fetch();

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient, times(2)).send(captor.capture(), any());
    assertEquals(
        "https://mycompany.okta.com/api/v1/logs?after=cursor-1",
        captor.getAllValues().get(1).uri().toString());
  }

  @Test
  void fetchThrowsOn401Unauthorized() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    doReturn(mockResponse(401, "unauthorized", null))
        .when(httpClient)
        .send(any(HttpRequest.class), any());

    OktaSourceFetcher fetcher =
        new OktaSourceFetcher(
            "mycompany.okta.com", "tok", "2026-01-01T00:00:00Z", null, 100, httpClient);

    RuntimeException ex = assertThrows(RuntimeException.class, fetcher::fetch);
    assertTrue(ex.getMessage().contains("invalid API token"));
  }

  @Test
  void fetchReturnsEmptyOnNon200NonAuthError() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    doReturn(mockResponse(500, "server error", null))
        .when(httpClient)
        .send(any(HttpRequest.class), any());

    OktaSourceFetcher fetcher =
        new OktaSourceFetcher(
            "mycompany.okta.com", "tok", "2026-01-01T00:00:00Z", null, 100, httpClient);

    assertTrue(fetcher.fetch().isEmpty());
  }

  @Test
  void fetchReturnsEmptyOnEmptyResponseAndAdvancesSinceTimestamp() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    doReturn(mockResponse(200, "[]", null)).when(httpClient).send(any(HttpRequest.class), any());

    OktaSourceFetcher fetcher =
        new OktaSourceFetcher(
            "mycompany.okta.com", "tok", "2026-01-01T00:00:00Z", null, 100, httpClient);

    assertTrue(fetcher.fetch().isEmpty());
  }

  @Test
  void streamingSourceIsNeverExhausted() {
    HttpClient httpClient = mock(HttpClient.class);
    OktaSourceFetcher fetcher =
        new OktaSourceFetcher("mycompany.okta.com", "tok", null, null, 100, httpClient);
    assertFalse(fetcher.isExhausted());
  }
}
