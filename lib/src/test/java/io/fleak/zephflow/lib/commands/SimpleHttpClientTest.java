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
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** Created by bolei on 9/6/24 */
class SimpleHttpClientTest {

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
