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
package io.fleak.zephflow.lib.metric;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.SplunkMetricSender;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class SplunkMetricSenderTest {

  @Mock private HttpClient mockHttpClient;

  @Mock private HttpResponse<String> mockHttpResponse;

  private SplunkMetricSender splunkMetricSender;

  @BeforeEach
  void setUp() throws IOException, InterruptedException {
    MockitoAnnotations.openMocks(this);

    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn("{\"text\":\"Success\",\"code\":0}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockHttpResponse);

    SplunkMetricSender.SplunkConfig config = new SplunkMetricSender.SplunkConfig();
    config.setHecUrl("http://localhost:8088/services/collector");
    config.setToken("test-token");
    config.setSource("test-source");
    config.setIndex("test-index");

    splunkMetricSender = new SplunkMetricSender(config, mockHttpClient);
  }

  @Test
  void sendMetricSuccess() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("tag2", "value2");

    splunkMetricSender.sendMetric("counter", "test_metric", 1, tags, additionalTags);

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricWithDoubleValue() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("tag2", "value2");

    splunkMetricSender.sendMetric("gauge", "testmetric", 1.5, tags, additionalTags);

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricWithBooleanValue() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    splunkMetricSender.sendMetric("status", "testmetric", true, tags, null);

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricWithStringValue() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    splunkMetricSender.sendMetric("info", "test_metric", "test_value", tags, null);

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricWithNullTags() throws IOException, InterruptedException {
    splunkMetricSender.sendMetric("counter", "test_metric", 1, null, null);

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricException() throws IOException, InterruptedException {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new IOException("Test exception"));

    Map<String, String> tags = new HashMap<>();
    Map<String, String> additionalTags = new HashMap<>();

    assertDoesNotThrow(
        () -> {
          splunkMetricSender.sendMetric("timer", "test_metric", 100, tags, additionalTags);
        });

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricNonSuccessResponse() throws IOException, InterruptedException {
    when(mockHttpResponse.statusCode()).thenReturn(400);
    when(mockHttpResponse.body()).thenReturn("{\"text\":\"Invalid token\",\"code\":4}");

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    assertDoesNotThrow(
        () -> {
          splunkMetricSender.sendMetric("counter", "test_metric", 1, tags, null);
        });

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricsSuccess() throws IOException, InterruptedException {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put("metric1", 100);
    metrics.put("metric2", 200.5);
    metrics.put("metric3", true);

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    splunkMetricSender.sendMetrics(metrics, tags);

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricsWithTimestamp() throws IOException, InterruptedException {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put("metric1", 100);
    metrics.put("metric2", 200.5);

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    long timestamp = System.currentTimeMillis();
    splunkMetricSender.sendMetrics(metrics, tags, timestamp);

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricsEmptyMetrics() throws IOException, InterruptedException {
    Map<String, Object> metrics = new HashMap<>();
    Map<String, String> tags = new HashMap<>();

    splunkMetricSender.sendMetrics(metrics, tags);

    verify(mockHttpClient, never())
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricsNullMetrics() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();

    splunkMetricSender.sendMetrics(null, tags);

    verify(mockHttpClient, never())
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricsException() throws IOException, InterruptedException {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new IOException("Test exception"));

    Map<String, Object> metrics = new HashMap<>();
    metrics.put("metric1", 100);

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    assertDoesNotThrow(
        () -> {
          splunkMetricSender.sendMetrics(metrics, tags);
        });

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void closeSuccess() {
    assertDoesNotThrow(
        () -> {
          splunkMetricSender.close();
        });
  }
}
