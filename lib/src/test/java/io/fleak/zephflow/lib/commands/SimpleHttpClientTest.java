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
package io.fleak.zephflow.lib.commands;

import static io.fleak.zephflow.lib.commands.SimpleHttpClient.HttpMethodType.GET;
import static io.fleak.zephflow.lib.commands.SimpleHttpClient.HttpMethodType.POST;
import static io.fleak.zephflow.lib.commands.SimpleHttpClient.MAX_RESPONSE_SIZE_BYTES;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.zip.GZIPOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/** Created by bolei on 9/6/24 */
class SimpleHttpClientTest {

  private static final String TEST_URL = "https://203.0.113.1/api/v2/logs";

  @ParameterizedTest
  @EnumSource(HttpClient.Version.class)
  void sendsExactBytesWithRequestOptionsAndReturnsResponseMetadata(HttpClient.Version version)
      throws Exception {
    byte[] payload;
    try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
      try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
        gzip.write("[{\"message\":\"zażółć 🔥\"}]".getBytes(StandardCharsets.UTF_8));
      }
      payload = output.toByteArray();
    }
    HttpClient httpClient = mock(HttpClient.class);
    LimitedSizeBodyHandler handler = new LimitedSizeBodyHandler(MAX_RESPONSE_SIZE_BYTES);
    HttpResponse<String> response = mock();
    when(response.statusCode()).thenReturn(429);
    when(response.headers())
        .thenReturn(HttpHeaders.of(Map.of("Retry-After", List.of("7")), (key, value) -> true));
    when(response.body()).thenReturn("retry later");
    when(httpClient.send(any(), eq(handler))).thenReturn(response);
    SimpleHttpClient client = new SimpleHttpClient(3, 1000L, httpClient, handler);

    HttpResponse<String> actual =
        client.sendHttpBytes(
            TEST_URL,
            POST,
            payload,
            List.of("Content-Type: application/json", "Content-Encoding: gzip", "DD-API-KEY: test"),
            version);

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient).send(requestCaptor.capture(), eq(handler));
    HttpRequest request = requestCaptor.getValue();
    assertEquals(TEST_URL, request.uri().toString());
    assertEquals("POST", request.method());
    assertEquals(version, request.version().orElseThrow());
    assertEquals(Duration.ofSeconds(60), request.timeout().orElseThrow());
    assertEquals("application/json", request.headers().firstValue("Content-Type").orElseThrow());
    assertEquals("gzip", request.headers().firstValue("Content-Encoding").orElseThrow());
    assertEquals("test", request.headers().firstValue("DD-API-KEY").orElseThrow());
    assertEquals(payload.length, request.bodyPublisher().orElseThrow().contentLength());
    assertArrayEquals(payload, readPublishedBytes(request));
    assertSame(response, actual);
    assertEquals(429, actual.statusCode());
    assertEquals("7", actual.headers().firstValue("Retry-After").orElseThrow());
    assertEquals("retry later", actual.body());
  }

  @ParameterizedTest
  @ValueSource(ints = {202, 302, 403, 503})
  void returnsEveryStatusWithoutInterpretingOrRetryingIt(int status) throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    LimitedSizeBodyHandler handler = new LimitedSizeBodyHandler(MAX_RESPONSE_SIZE_BYTES);
    HttpResponse<String> response = mock();
    when(response.statusCode()).thenReturn(status);
    when(response.body()).thenReturn("");
    when(httpClient.send(any(), eq(handler))).thenReturn(response);
    SimpleHttpClient client = new SimpleHttpClient(3, 1000L, httpClient, handler);

    HttpResponse<String> actual =
        client.sendHttpBytes(TEST_URL, POST, new byte[0], List.of(), HttpClient.Version.HTTP_2);

    assertEquals(status, actual.statusCode());
    assertEquals("", actual.body());
    verify(httpClient).send(any(), eq(handler));
  }

  @ParameterizedTest
  @ValueSource(strings = {"connection closed", "RST_STREAM"})
  void propagatesIoExceptionWithoutRetryOrVersionFallback(String message) throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    LimitedSizeBodyHandler handler = new LimitedSizeBodyHandler(MAX_RESPONSE_SIZE_BYTES);
    IOException failure = new IOException(message);
    when(httpClient.send(any(), eq(handler))).thenThrow(failure);
    SimpleHttpClient client = new SimpleHttpClient(3, 1000L, httpClient, handler);

    assertSame(
        failure,
        assertThrows(
            IOException.class,
            () ->
                client.sendHttpBytes(
                    TEST_URL, POST, new byte[0], List.of(), HttpClient.Version.HTTP_2)));

    verify(httpClient).send(any(), eq(handler));
  }

  @Test
  void propagatesInterruptionWithoutRetry() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    LimitedSizeBodyHandler handler = new LimitedSizeBodyHandler(MAX_RESPONSE_SIZE_BYTES);
    InterruptedException failure = new InterruptedException("cancelled");
    when(httpClient.send(any(), eq(handler))).thenThrow(failure);
    SimpleHttpClient client = new SimpleHttpClient(3, 1000L, httpClient, handler);

    assertSame(
        failure,
        assertThrows(
            InterruptedException.class,
            () ->
                client.sendHttpBytes(
                    TEST_URL, POST, new byte[0], List.of(), HttpClient.Version.HTTP_2)));

    verify(httpClient).send(any(), eq(handler));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"http://127.0.0.1/logs", "http://10.1.2.3/logs", "file:///tmp/logs", "not a URL"})
  void rejectsDisallowedUrlsBeforeSending(String url) {
    HttpClient httpClient = mock(HttpClient.class);
    SimpleHttpClient client =
        new SimpleHttpClient(
            3, 1000L, httpClient, new LimitedSizeBodyHandler(MAX_RESPONSE_SIZE_BYTES));

    assertThrows(
        SecurityException.class,
        () -> client.sendHttpBytes(url, POST, new byte[0], List.of(), HttpClient.Version.HTTP_2));

    verifyNoInteractions(httpClient);
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing separator", "invalid header: value"})
  void rejectsMalformedHeadersBeforeSending(String header) {
    HttpClient httpClient = mock(HttpClient.class);
    SimpleHttpClient client =
        new SimpleHttpClient(
            3, 1000L, httpClient, new LimitedSizeBodyHandler(MAX_RESPONSE_SIZE_BYTES));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            client.sendHttpBytes(
                TEST_URL, POST, new byte[0], List.of(header), HttpClient.Version.HTTP_2));

    verifyNoInteractions(httpClient);
  }

  @Test
  void rejectsResponseBeyondConfiguredByteLimit() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    LimitedSizeBodyHandler handler = new LimitedSizeBodyHandler(4);
    when(httpClient.send(any(), eq(handler)))
        .thenAnswer(
            invocation -> {
              receiveBody(invocation.getArgument(1), "12345");
              throw new AssertionError("Oversized response was accepted");
            });
    SimpleHttpClient client = new SimpleHttpClient(3, 1000L, httpClient, handler);

    IOException failure =
        assertThrows(
            IOException.class,
            () ->
                client.sendHttpBytes(
                    TEST_URL, POST, new byte[0], List.of(), HttpClient.Version.HTTP_2));

    assertEquals("Response body exceeds max size of 4 bytes.", failure.getMessage());
    verify(httpClient).send(any(), eq(handler));
  }

  @Test
  void acceptsResponseAtConfiguredByteLimit() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    LimitedSizeBodyHandler handler = new LimitedSizeBodyHandler(4);
    when(httpClient.send(any(), eq(handler)))
        .thenAnswer(
            invocation -> {
              String body = receiveBody(invocation.getArgument(1), "1234");
              HttpResponse<String> response = mock();
              when(response.body()).thenReturn(body);
              return response;
            });
    SimpleHttpClient client = new SimpleHttpClient(3, 1000L, httpClient, handler);

    HttpResponse<String> response =
        client.sendHttpBytes(TEST_URL, POST, new byte[0], List.of(), HttpClient.Version.HTTP_2);

    assertEquals("1234", response.body());
    verify(httpClient).send(any(), eq(handler));
  }

  @Test
  void sharedClientKeepsRedirectsDisabledAndConnectionTimeout() throws Exception {
    SimpleHttpClient client = SimpleHttpClient.getInstance(MAX_RESPONSE_SIZE_BYTES);
    Field field = SimpleHttpClient.class.getDeclaredField("httpClient");
    field.setAccessible(true);
    HttpClient httpClient = (HttpClient) field.get(client);

    assertEquals(HttpClient.Redirect.NEVER, httpClient.followRedirects());
    assertEquals(Duration.ofSeconds(60), httpClient.connectTimeout().orElseThrow());
  }

  @Test
  void legacyStringCallKeepsIoRetriesAndUtf8Body() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    LimitedSizeBodyHandler handler = new LimitedSizeBodyHandler(MAX_RESPONSE_SIZE_BYTES);
    HttpResponse<String> response = mock();
    when(response.statusCode()).thenReturn(202);
    when(response.body()).thenReturn("accepted");
    when(httpClient.send(any(), eq(handler)))
        .thenThrow(new IOException("first"))
        .thenThrow(new IOException("second"))
        .thenReturn(response);
    SimpleHttpClient client = new SimpleHttpClient(3, 0L, httpClient, handler);

    assertEquals(
        "accepted", client.callHttpEndpoint(TEST_URL, POST, "zażółć 🔥", List.of("key: value")));

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient, times(3)).send(requestCaptor.capture(), eq(handler));
    for (HttpRequest request : requestCaptor.getAllValues()) {
      assertArrayEquals("zażółć 🔥".getBytes(StandardCharsets.UTF_8), readPublishedBytes(request));
      assertEquals(HttpClient.Version.HTTP_2, request.version().orElseThrow());
    }
  }

  @Test
  void legacyStringCallKeepsHttp1FallbackAndItsRetryBudget() throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    LimitedSizeBodyHandler handler = new LimitedSizeBodyHandler(MAX_RESPONSE_SIZE_BYTES);
    HttpResponse<String> response = mock();
    when(response.statusCode()).thenReturn(202);
    when(response.body()).thenReturn("accepted");
    when(httpClient.send(any(), eq(handler)))
        .thenThrow(new IOException("first"))
        .thenThrow(new IOException("second"))
        .thenThrow(new IOException("RST_STREAM"))
        .thenThrow(new IOException("fourth"))
        .thenThrow(new IOException("fifth"))
        .thenReturn(response);
    SimpleHttpClient client = new SimpleHttpClient(3, 0L, httpClient, handler);

    assertEquals("accepted", client.callHttpEndpoint(TEST_URL, POST, null, List.of()));

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient, times(6)).send(requestCaptor.capture(), eq(handler));
    assertEquals(
        List.of(
            HttpClient.Version.HTTP_2,
            HttpClient.Version.HTTP_2,
            HttpClient.Version.HTTP_2,
            HttpClient.Version.HTTP_1_1,
            HttpClient.Version.HTTP_1_1,
            HttpClient.Version.HTTP_1_1),
        requestCaptor.getAllValues().stream()
            .map(request -> request.version().orElseThrow())
            .toList());
    assertArrayEquals(new byte[0], readPublishedBytes(requestCaptor.getValue()));
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 302, 403, 503})
  void legacyStringCallKeepsStatusHandlingWithoutStatusRetries(int status) throws Exception {
    HttpClient httpClient = mock(HttpClient.class);
    LimitedSizeBodyHandler handler = new LimitedSizeBodyHandler(MAX_RESPONSE_SIZE_BYTES);
    HttpResponse<String> response = mock();
    when(response.statusCode()).thenReturn(status);
    when(response.body()).thenReturn("response");
    when(httpClient.send(any(), eq(handler))).thenReturn(response);
    SimpleHttpClient client = new SimpleHttpClient(3, 0L, httpClient, handler);

    if (status >= 400) {
      RuntimeException failure =
          assertThrows(
              RuntimeException.class,
              () -> client.callHttpEndpoint(TEST_URL, GET, null, List.of()));
      assertEquals("response", failure.getMessage());
    } else {
      assertEquals("response", client.callHttpEndpoint(TEST_URL, GET, null, List.of()));
    }
    verify(httpClient).send(any(), eq(handler));
  }

  private static byte[] readPublishedBytes(HttpRequest request) {
    HttpResponse.BodySubscriber<byte[]> subscriber = HttpResponse.BodySubscribers.ofByteArray();
    request
        .bodyPublisher()
        .orElseThrow()
        .subscribe(
            new Flow.Subscriber<>() {
              @Override
              public void onSubscribe(Flow.Subscription subscription) {
                subscriber.onSubscribe(subscription);
              }

              @Override
              public void onNext(ByteBuffer item) {
                subscriber.onNext(List.of(item));
              }

              @Override
              public void onError(Throwable throwable) {
                subscriber.onError(throwable);
              }

              @Override
              public void onComplete() {
                subscriber.onComplete();
              }
            });
    return subscriber.getBody().toCompletableFuture().join();
  }

  private static String receiveBody(HttpResponse.BodyHandler<String> handler, String body)
      throws IOException, InterruptedException {
    HttpResponse.BodySubscriber<String> subscriber = handler.apply(mock());
    subscriber.onSubscribe(mock(Flow.Subscription.class));
    subscriber.onNext(List.of(ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8))));
    subscriber.onComplete();
    try {
      return subscriber.getBody().toCompletableFuture().get();
    } catch (ExecutionException e) {
      throw (IOException) e.getCause();
    }
  }

  @Test
  @Disabled
  void httpCall() {
    callAndPrintResponse(
        "https://jsonplaceholder.typicode.com/posts",
        GET,
        StringUtils.EMPTY,
        List.of("Content-Type: application/json"));
  }

  @Test
  @Disabled
  void httpCall_responseExceedsSizeLimit() {
    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () ->
                callAndPrintResponse(
                    "https://jsonplaceholder.typicode.com/posts",
                    GET,
                    StringUtils.EMPTY,
                    List.of("Content-Type: application/json"),
                    100));
    assertEquals("Response body exceeds max size of 100 bytes.", e.getMessage());
  }

  @Disabled
  @Test
  public void callOpenAiApi() {
    String key = System.getenv("OPENAI_API_KEY");
    callAndPrintResponse(
        "https://api.openai.com/v1/chat/completions",
        POST,
        """
            {
                "model": "gpt-4o-mini",
                "messages": [
                  {
                    "role": "system",
                    "content": "You are a helpful assistant."
                  },
                  {
                    "role": "user",
                    "content": "Who won the world series in 2020?"
                  },
                  {
                    "role": "assistant",
                    "content": "The Los Angeles Dodgers won the World Series in 2020."
                  },
                  {
                    "role": "user",
                    "content": "Where was it played?"
                  }
                ]
              }""",
        List.of("Content-Type: application/json", "Authorization: Bearer " + key));
  }

  private void callAndPrintResponse(
      String url,
      SimpleHttpClient.HttpMethodType method,
      String body,
      List<String> headers,
      int maxSize) {
    SimpleHttpClient client = SimpleHttpClient.getInstance(maxSize);
    String resp = client.callHttpEndpoint(url, method, body, headers);
    System.out.println("Response Body: " + resp);
  }

  private void callAndPrintResponse(
      String url, SimpleHttpClient.HttpMethodType method, String body, List<String> headers) {
    callAndPrintResponse(url, method, body, headers, MAX_RESPONSE_SIZE_BYTES);
  }

  @Test
  public void testHttpWithRetry() throws IOException, InterruptedException {
    //noinspection unchecked
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("test_response");

    HttpClient httpClient = mock(HttpClient.class);
    LimitedSizeBodyHandler bodyHandler = mock();
    when(httpClient.send(any(), eq(bodyHandler)))
        .thenThrow(new IOException("forced error"))
        .thenThrow(new IOException("forced error"))
        .thenReturn(mockResponse);
    SimpleHttpClient simpleHttpClient = new SimpleHttpClient(3, 1000L, httpClient, bodyHandler);
    long start = System.currentTimeMillis();
    String responseBody =
        simpleHttpClient.callHttpEndpoint(
            "http://www.google.com", GET, "\"foo\"", List.of("key: bar"));
    long elapse = System.currentTimeMillis() - start; // backoff twice: 1s, 2s
    assertEquals("test_response", responseBody);
    System.out.println(elapse);
    assertTrue(elapse >= 3000L);
  }
}
