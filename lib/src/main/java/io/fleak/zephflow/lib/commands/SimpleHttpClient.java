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

import static java.time.temporal.ChronoUnit.MINUTES;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fleak.zephflow.lib.utils.MiscUtils;
import io.fleak.zephflow.lib.utils.SecurityUtils;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/** Created by bolei on 9/6/24 */
@Slf4j
public class SimpleHttpClient {

  public static final int MAX_RESPONSE_SIZE_BYTES = 10 * 1024 * 1024;
  private static final Duration DEFAULT_HTTP_TIMEOUT =
      Duration.of(1, MINUTES); // TODO make it configurable
  private static SimpleHttpClient instance;

  private final int maxRetries;
  private final long delay;
  private final HttpClient httpClient;
  private final LimitedSizeBodyHandler handler;

  @VisibleForTesting
  public SimpleHttpClient(
      int maxRetries, long delay, HttpClient httpClient, LimitedSizeBodyHandler handler) {
    this.maxRetries = maxRetries;
    this.delay = delay;
    this.httpClient = httpClient;
    this.handler = handler;
  }

  public static SimpleHttpClient getInstance(int maxResponseSizeBytes) {
    if (instance == null) {
      LimitedSizeBodyHandler handler = new LimitedSizeBodyHandler(maxResponseSizeBytes);
      HttpClient httpClient =
          HttpClient.newBuilder()
              .connectTimeout(DEFAULT_HTTP_TIMEOUT)
              .followRedirects(HttpClient.Redirect.NEVER) // Disable redirects
              .build();
      instance = new SimpleHttpClient(3, 1000, httpClient, handler);
    }
    return instance;
  }

  public String callHttpEndpoint(
      @NonNull String url,
      @NonNull HttpMethodType method,
      String requestBody,
      @NonNull List<String> headerEntries) {
    if (!SecurityUtils.isUrlAllowed(url)) {
      throw new SecurityException("Unauthorized URL access: " + url);
    }
    return callHttpEndpointNoSecureCheck(url, method, requestBody, headerEntries);
  }

  public String callHttpEndpointNoSecureCheck(
      @NonNull String url,
      @NonNull HttpMethodType method,
      String requestBody,
      @NonNull List<String> headerEntries) {

    Map<String, String> parsedHeaders = parseHeaders(headerEntries);

    HttpRequest request =
        buildRequest(url, method, requestBody, parsedHeaders, HttpClient.Version.HTTP_2);
    HttpResponse<String> httpResponse;
    try {
      httpResponse = callHttpWithExponentialRetry(request, url, method, requestBody, parsedHeaders);
    } catch (UseHTTP1Exception useHTTP1Exception) {
      // on IO Exception, try again using Http1_1
      request = buildRequest(url, method, requestBody, parsedHeaders, HttpClient.Version.HTTP_1_1);
      try {
        httpResponse =
            callHttpWithExponentialRetry(request, url, method, requestBody, parsedHeaders);
      } catch (Exception e) {
        log.debug("Try version downgrade to HTTP1 failed {}", url, e);
        throw new RuntimeException(useHTTP1Exception.getCause());
      }
    }

    int code = httpResponse.statusCode();
    if (code >= 400) {
      log.debug(
          "http response\ncode: {}\nbody: {}\n request: http({}, {}, {}, {})",
          code,
          httpResponse.body(),
          url,
          method,
          requestBody,
          parsedHeaders);
      throw new RuntimeException(httpResponse.body());
    }
    return httpResponse.body();
  }

  private static HttpRequest buildRequest(
      String url,
      HttpMethodType method,
      String requestBody,
      Map<String, String> parsedHeaders,
      HttpClient.Version version) {
    HttpRequest.Builder requestBuilder =
        HttpRequest.newBuilder()
            .version(version)
            .uri(URI.create(url))
            .timeout(DEFAULT_HTTP_TIMEOUT)
            .method(
                method.toString(),
                HttpRequest.BodyPublishers.ofString(Optional.ofNullable(requestBody).orElse("")));

    parsedHeaders.forEach(requestBuilder::header);
    return requestBuilder.build();
  }

  private HttpResponse<String> callHttpWithExponentialRetry(
      HttpRequest request,
      String url,
      HttpMethodType method,
      String requestBody,
      Map<String, String> headerEntries) {
    long currentDelay = delay;
    Throwable exception = null;
    for (int i = 0; i < maxRetries; i++) {
      try {
        return httpClient.send(request, handler);
      } catch (Exception e) {
        log.debug(
            "retry={}, failed to invoke http({}, {}, {}, {})",
            i,
            url,
            method,
            requestBody,
            headerEntries,
            e);

        if (e instanceof IOException ioException && ioException.toString().contains("RST_STREAM")) {
          throw new UseHTTP1Exception(ioException);
        }
        // Exponential backoff: double the delay and wait
        if (i >= maxRetries - 1) { // Don't sleep after the last attempt
          exception = e;
          break;
        }
        MiscUtils.threadSleep(currentDelay);
        currentDelay *= 2;
      }
    } // If this was the last attempt, re-throw the exception
    Preconditions.checkNotNull(exception);
    throw new RuntimeException(exception.getMessage(), exception);
  }

  static Map<String, String> parseHeaders(List<String> headers) {
    return headers.stream()
        .map(
            header -> {
              int colonPos = header.indexOf(':');
              if (colonPos <= 0) {
                throw new IllegalArgumentException("Invalid header format");
              }
              String headerKey = header.substring(0, colonPos).trim();
              String headerVal = header.substring(colonPos + 1).trim();

              return Pair.of(headerKey, headerVal);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public enum HttpMethodType {
    GET,
    POST,
    PUT,
    DELETE
  }

  public static class UseHTTP1Exception extends RuntimeException {
    public UseHTTP1Exception(Throwable cause) {
      super(cause);
    }
  }
}
