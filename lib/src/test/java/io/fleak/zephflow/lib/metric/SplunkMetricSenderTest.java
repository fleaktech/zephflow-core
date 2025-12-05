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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.SplunkMetricSender;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class SplunkMetricSenderTest {

  @Mock private HttpClient mockHttpClient;

  @Mock private HttpResponse<String> mockHttpResponse;

  private SplunkMetricSender splunkMetricSender;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);

    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn("{\"text\":\"Success\",\"code\":0}");
    when(mockHttpClient.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(CompletableFuture.completedFuture(mockHttpResponse));

    SplunkMetricSender.SplunkConfig.SplunkConfigBuilder config =
        SplunkMetricSender.SplunkConfig.builder();
    config.hecUrl("http://localhost:8088/services/collector");
    config.token("test-token");
    config.source("test-source");
    config.index("test-index");

    splunkMetricSender = new SplunkMetricSender(config.build(), mockHttpClient);
  }

  @Test
  void sendMetricSuccess() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("tag2", "value2");

    splunkMetricSender.sendMetric("counter", "test_metric", 1, tags, additionalTags);
    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricWithDoubleValue() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("tag2", "value2");

    splunkMetricSender.sendMetric("gauge", "testmetric", 1.5, tags, additionalTags);
    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricWithBooleanValue() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    splunkMetricSender.sendMetric("status", "testmetric", true, tags, null);
    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricWithStringValue() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    splunkMetricSender.sendMetric("info", "test_metric", "test_value", tags, null);
    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricWithNullTags() {
    splunkMetricSender.sendMetric("counter", "test_metric", 1, null, null);
    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricException() throws InterruptedException {
    when(mockHttpClient.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(CompletableFuture.failedFuture(new IOException("Test exception")));

    Map<String, String> tags = new HashMap<>();
    Map<String, String> additionalTags = new HashMap<>();

    assertDoesNotThrow(
        () -> {
          splunkMetricSender.sendMetric("timer", "test_metric", 100, tags, additionalTags);
          splunkMetricSender.flush();
          Thread.sleep(2000); // Wait for retries to complete (async with delays)
          splunkMetricSender.close();
        });

    verify(mockHttpClient, times(4))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricNonSuccessResponse() {
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
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricsSuccess() {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put("metric1", 100);
    metrics.put("metric2", 200.5);
    metrics.put("metric3", true);

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    splunkMetricSender.sendMetrics(metrics, tags);
    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricsWithTimestamp() {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put("metric1", 100);
    metrics.put("metric2", 200.5);

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    long timestamp = System.currentTimeMillis();
    splunkMetricSender.sendMetrics(metrics, tags, timestamp);
    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricsEmptyMetrics() {
    Map<String, Object> metrics = new HashMap<>();
    Map<String, String> tags = new HashMap<>();

    splunkMetricSender.sendMetrics(metrics, tags);

    verify(mockHttpClient, never())
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricsNullMetrics() {
    Map<String, String> tags = new HashMap<>();

    splunkMetricSender.sendMetrics(null, tags);

    verify(mockHttpClient, never())
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void sendMetricsException() throws InterruptedException {
    when(mockHttpClient.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(CompletableFuture.failedFuture(new IOException("Test exception")));

    Map<String, Object> metrics = new HashMap<>();
    metrics.put("metric1", 100);

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    assertDoesNotThrow(
        () -> {
          splunkMetricSender.sendMetrics(metrics, tags);
          splunkMetricSender.flush();
          Thread.sleep(2000); // Wait for retries to complete (async with delays)
          splunkMetricSender.close();
        });

    verify(mockHttpClient, times(4))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
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
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
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
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
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
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    splunkMetricSender.flush();
    Thread.sleep(500);

    verify(mockHttpClient, times(1))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
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
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    splunkMetricSender.close();

    verify(mockHttpClient, times(1))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void flushEmptyBatchDoesNothing() throws IOException, InterruptedException {
    splunkMetricSender.flush();
    Thread.sleep(100);

    verify(mockHttpClient, never())
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
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
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    // Wait for time-based flush (5 seconds + buffer)
    Thread.sleep(5000);

    // Should have flushed by now
    verify(mockHttpClient, times(1))
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
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
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
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
        .sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void droppedMetricsCounter() throws IOException, InterruptedException {
    SplunkMetricSender.SplunkConfig smallQueueConfig =
        SplunkMetricSender.SplunkConfig.builder()
            .hecUrl("http://localhost:8088/services/collector")
            .token("test-token")
            .source("test-source")
            .index("test-index")
            .queueCapacity(1000)
            .build();

    SplunkMetricSender smallQueueSender = new SplunkMetricSender(smallQueueConfig, mockHttpClient);

    when(mockHttpClient.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              Thread.sleep(200);
              return CompletableFuture.completedFuture(mockHttpResponse);
            });

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    for (int i = 0; i < 1200; i++) {
      smallQueueSender.sendMetric("counter", "metric_" + i, i, tags, null);
    }

    long droppedCount = smallQueueSender.getDroppedMetricsCount();
    assertTrue(
        droppedCount > 0,
        "Expected that some metrics were dropped, but was dropped " + droppedCount);

    smallQueueSender.close();
  }

  @Test
  void realSplunkIntegrationTest() throws InterruptedException {
    String hecUrl = System.getenv("SPLUNK_HEC_URL");
    String hecToken = System.getenv("SPLUNK_HEC_TOKEN");
    String index = System.getenv("SPLUNK_INDEX");

    assumeTrue(
        hecUrl != null && !hecUrl.isEmpty(),
        "SPLUNK_HEC_URL environment variable not set - skipping integration test");
    assumeTrue(
        hecToken != null && !hecToken.isEmpty(),
        "SPLUNK_HEC_TOKEN environment variable not set - skipping integration test");
    assumeTrue(
        index != null && !index.isEmpty(),
        "SPLUNK_INDEX environment variable not set - skipping integration test");

    System.out.println("Running integration test against Splunk instance: " + hecUrl);

    SplunkMetricSender.SplunkConfig config =
        SplunkMetricSender.SplunkConfig.builder()
            .hecUrl(hecUrl)
            .token(hecToken)
            .source("integration-test")
            .index(index)
            .batchSize(10)
            .flushIntervalSeconds(2)
            .maxRetries(2)
            .httpRequestTimeoutSeconds(10)
            .shutdownTimeoutSeconds(15)
            .build();

    SplunkMetricSender sender = new SplunkMetricSender(config);

    try {
      long timestamp = System.currentTimeMillis();
      String testId = "test_" + timestamp;

      Map<String, String> tags = new HashMap<>();
      tags.put("environment", "integration-test");
      tags.put("test_id", testId);
      tags.put("test_run", String.valueOf(timestamp));

      System.out.println("Sending individual metrics...");
      assertTrue(
          sender.sendMetric("counter", "integration_counter", 42, tags, null),
          "Failed to enqueue counter metric");
      assertTrue(
          sender.sendMetric("gauge", "integration_gauge", 99.5, tags, null),
          "Failed to enqueue gauge metric");
      assertTrue(
          sender.sendMetric("timer", "integration_timer", 1234L, tags, null),
          "Failed to enqueue timer metric");
      assertTrue(
          sender.sendMetric("status", "integration_status", true, tags, null),
          "Failed to enqueue boolean metric");
      assertTrue(
          sender.sendMetric("info", "integration_info", "test_value", tags, null),
          "Failed to enqueue string metric");

      System.out.println("Sending batch metrics...");
      Map<String, Object> batchMetrics = new HashMap<>();
      batchMetrics.put("batch_metric_1", 100);
      batchMetrics.put("batch_metric_2", 200.5);
      batchMetrics.put("batch_metric_3", 300L);
      batchMetrics.put("batch_metric_4", true);
      batchMetrics.put("batch_metric_5", "batch_value");

      assertTrue(
          sender.sendMetrics(batchMetrics, tags, timestamp), "Failed to enqueue batch metrics");

      System.out.println("Sending metrics to trigger batch flush...");
      for (int i = 0; i < 15; i++) {
        sender.sendMetric("counter", "bulk_metric_" + i, i * 10, tags, null);
      }

      System.out.println("Triggering manual flush...");
      assertTrue(sender.flush(), "Flush operation failed");

      Thread.sleep(1000);

      SplunkMetricSender.MetricStats stats = sender.getStats();
      System.out.println("Stats after sending:");
      System.out.println("  Enqueued: " + stats.getMetricsEnqueued());
      System.out.println("  Sent: " + stats.getMetricsSent());
      System.out.println("  Batches: " + stats.getBatchesSent());
      System.out.println("  Dropped: " + stats.getMetricsDropped());
      System.out.println("  Retries: " + stats.getTotalRetries());
      System.out.println("  Failures: " + stats.getTotalFailures());

      assertTrue(
          stats.getMetricsEnqueued() >= 25,
          "Expected at least 25 metrics enqueued, got: " + stats.getMetricsEnqueued());

      assertEquals(
          0,
          stats.getMetricsDropped(),
          "No metrics should be dropped in integration test, but got: "
              + stats.getMetricsDropped());

      System.out.println("Waiting for metrics to be sent...");
      Thread.sleep(3000);

      stats = sender.getStats();
      System.out.println("Final stats:");
      System.out.println("  Enqueued: " + stats.getMetricsEnqueued());
      System.out.println("  Sent: " + stats.getMetricsSent());
      System.out.println("  Batches: " + stats.getBatchesSent());
      System.out.println("  Failures: " + stats.getTotalFailures());

      assertTrue(
          stats.getMetricsSent() > 0,
          "Expected metrics to be sent to Splunk, but sent count is: " + stats.getMetricsSent());

      assertTrue(
          stats.getBatchesSent() > 0,
          "Expected batches to be sent to Splunk, but batch count is: " + stats.getBatchesSent());

      System.out.println(
          "\n✅ Integration test PASSED - Successfully sent "
              + stats.getMetricsSent()
              + " metrics in "
              + stats.getBatchesSent()
              + " batches to Splunk");
      System.out.println(
          "   Search in Splunk with: index="
              + index
              + " source=integration-test test_id="
              + testId);

    } finally {
      System.out.println("Closing sender...");
      sender.close();
      System.out.println("Sender closed");
    }
  }
}
