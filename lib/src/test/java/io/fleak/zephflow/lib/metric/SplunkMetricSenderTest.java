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
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    SplunkMetricSender.SplunkConfig.SplunkConfigBuilder config =
        SplunkMetricSender.SplunkConfig.builder();
    config.hecUrl("http://localhost:8088/services/collector");
    config.token("test-token");
    config.source("test-source");
    config.index("test-index");

    splunkMetricSender = new SplunkMetricSender(config.build(), mockHttpClient);
  }

  @Test
  void sendMetricSuccess() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("tag2", "value2");

    splunkMetricSender.sendMetric("counter", "test_metric", 1, tags, additionalTags);
    splunkMetricSender.close();

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
    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricWithBooleanValue() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    splunkMetricSender.sendMetric("status", "testmetric", true, tags, null);
    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricWithStringValue() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    splunkMetricSender.sendMetric("info", "test_metric", "test_value", tags, null);
    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricWithNullTags() throws IOException, InterruptedException {
    splunkMetricSender.sendMetric("counter", "test_metric", 1, null, null);
    splunkMetricSender.close();

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
          splunkMetricSender.close();
        });

    // With retry logic: 1 initial attempt + 3 retries = 4 total attempts
    verify(mockHttpClient, times(4))
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
          splunkMetricSender.close();
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
    splunkMetricSender.close();

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
    splunkMetricSender.close();

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
          splunkMetricSender.close();
        });

    // With retry logic: 1 initial attempt + 3 retries = 4 total attempts
    verify(mockHttpClient, times(4))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void closeSuccess() {
    assertDoesNotThrow(
        () -> {
          splunkMetricSender.close();
        });
  }

  @Test
  void batchAutoFlushAt100Metrics() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    for (int i = 0; i < 100; i++) {
      splunkMetricSender.sendMetric("counter", "metric_" + i, i, tags, null);
    }

    Thread.sleep(500);

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void batchMultipleAutoFlushes() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    for (int i = 0; i < 250; i++) {
      splunkMetricSender.sendMetric("counter", "metric_" + i, i, tags, null);
    }

    Thread.sleep(500);

    verify(mockHttpClient, times(2))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void batchManualFlushWithPartialBatch() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    for (int i = 0; i < 50; i++) {
      splunkMetricSender.sendMetric("counter", "metric_" + i, i, tags, null);
    }

    Thread.sleep(100);

    verify(mockHttpClient, never())
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    splunkMetricSender.flush();
    Thread.sleep(500);

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void closeFlushesRemainingMetrics() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    // Send 30 metrics (less than batch size)
    for (int i = 0; i < 30; i++) {
      splunkMetricSender.sendMetric("counter", "metric_" + i, i, tags, null);
    }

    verify(mockHttpClient, never())
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void flushEmptyBatchDoesNothing() throws IOException, InterruptedException {
    splunkMetricSender.flush();
    Thread.sleep(100);

    verify(mockHttpClient, never())
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void batchTimeBasedAutoFlush() throws IOException, InterruptedException {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    // Send 30 metrics (less than batch size of 100)
    for (int i = 0; i < 30; i++) {
      splunkMetricSender.sendMetric("counter", "metric_" + i, i, tags, null);
    }

    // Should not flush yet
    Thread.sleep(1000);
    verify(mockHttpClient, never())
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    // Wait for time-based flush (5 seconds + buffer)
    Thread.sleep(5000);

    // Should have flushed by now
    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void retryOnServerError() throws IOException, InterruptedException {
    // Return 503 three times, then 200
    when(mockHttpResponse.statusCode()).thenReturn(503, 503, 503, 200);
    when(mockHttpResponse.body())
        .thenReturn(
            "Service Unavailable",
            "Service Unavailable",
            "Service Unavailable",
            "{\"text\":\"Success\",\"code\":0}");

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    // Send 100 metrics to trigger immediate batch send (batch size is 100)
    for (int i = 0; i < 100; i++) {
      splunkMetricSender.sendMetric("counter", "metric_" + i, i, tags, null);
    }

    // Wait for batch processing and retries to complete
    Thread.sleep(2000);

    splunkMetricSender.close();

    // Should retry 3 times and succeed on 4th attempt
    verify(mockHttpClient, times(4))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void noRetryOnClientError() throws IOException, InterruptedException {
    when(mockHttpResponse.statusCode()).thenReturn(401);
    when(mockHttpResponse.body()).thenReturn("Unauthorized");

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    splunkMetricSender.sendMetric("counter", "test_metric", 1, tags, null);
    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void droppedMetricsCounter() throws IOException, InterruptedException {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              Thread.sleep(200); // Delay each HTTP call
              return mockHttpResponse;
            });

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    for (int i = 0; i < 1200; i++) {
      splunkMetricSender.sendMetric("counter", "metric_" + i, i, tags, null);
    }

    long droppedCount = splunkMetricSender.getDroppedMetricsCount();
    assertTrue(
        droppedCount > 0,
        "Expected that some metrics ware dropped, but was dropped " + droppedCount);

    splunkMetricSender.close();
  }
}
