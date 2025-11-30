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
package io.fleak.zephflow.api.metric;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class SplunkMetricSender implements AutoCloseable {

  @Data
  public static class SplunkConfig {
    private String hecUrl;
    private String token;
    private String source;
    private String index;

    private int batchSize = 100;
    private int queueCapacity = 1000;
    private long flushIntervalSeconds = 5;
    private int maxRetries = 3;
    private long initialRetryDelayMs = 100;
    private long maxRetryDelayMs = 30000;
    private int httpConnectTimeoutSeconds = 10;
    private int httpRequestTimeoutSeconds = 30;
    private int shutdownTimeoutSeconds = 10;
    private int flushTimeoutSeconds = 5;
  }

  @Data
  public static class MetricStats {
    private final long metricsEnqueued;
    private final long metricsSent;
    private final long batchesSent;
    private final long metricsDropped;
    private final long totalRetries;
    private final long totalFailures;
    private final int currentQueueSize;
    private final int queueCapacity;
  }

  private final int batchSize;
  private final int queueCapacity;
  private final long flushIntervalSeconds;
  private final int maxRetries;
  private final long initialRetryDelayMs;
  private final long maxRetryDelayMs;
  private final int httpRequestTimeoutSeconds;
  private final int shutdownTimeoutSeconds;
  private final int flushTimeoutSeconds;

  private final HttpClient httpClient;
  private final String hecUrl;
  private final String token;
  private final String source;
  private final String index;
  private final ObjectMapper objectMapper;
  private final LinkedBlockingQueue<Map<String, Object>> metricQueue;
  private final ExecutorService workerExecutor;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicReference<CountDownLatch> flushLatch = new AtomicReference<>();

  private final AtomicLong metricsEnqueuedCount = new AtomicLong(0);
  private final AtomicLong metricsSentCount = new AtomicLong(0);
  private final AtomicLong batchesSentCount = new AtomicLong(0);
  private final AtomicLong droppedMetricsCount = new AtomicLong(0);
  private final AtomicLong totalRetriesCount = new AtomicLong(0);
  private final AtomicLong totalFailuresCount = new AtomicLong(0);

  public SplunkMetricSender(SplunkConfig config) {
    this(config, createDefaultHttpClient(config));
  }

  public SplunkMetricSender(SplunkConfig config, HttpClient httpClient) {
    validateConfig(config);

    this.hecUrl = config.getHecUrl();
    this.token = config.getToken();
    this.source = config.getSource() != null ? config.getSource() : "zephflow";
    this.index = config.getIndex();

    this.batchSize = config.getBatchSize();
    this.queueCapacity = config.getQueueCapacity();
    this.flushIntervalSeconds = config.getFlushIntervalSeconds();
    this.maxRetries = config.getMaxRetries();
    this.initialRetryDelayMs = config.getInitialRetryDelayMs();
    this.maxRetryDelayMs = config.getMaxRetryDelayMs();
    this.httpRequestTimeoutSeconds = config.getHttpRequestTimeoutSeconds();
    this.shutdownTimeoutSeconds = config.getShutdownTimeoutSeconds();
    this.flushTimeoutSeconds = config.getFlushTimeoutSeconds();

    this.httpClient = httpClient;
    this.objectMapper = configureObjectMapper();
    this.metricQueue = new LinkedBlockingQueue<>(queueCapacity);
    this.workerExecutor = createWorkerExecutor();

    try {
      log.info(
          "Splunk Metric Sender initialized (destination={}, batchSize={}, queueCapacity={}, flushInterval={}s, maxRetries={}, maxRetryDelay={}ms)",
          maskUrl(hecUrl),
          batchSize,
          queueCapacity,
          flushIntervalSeconds,
          maxRetries,
          maxRetryDelayMs);

      startBackgroundWorker();
    } catch (Exception e) {
      workerExecutor.shutdownNow();
      throw new IllegalStateException("Failed to initialize Splunk Metric Sender", e);
    }
  }

  public boolean sendMetric(
      String type,
      String name,
      Object value,
      Map<String, String> tags,
      Map<String, String> additionalTags) {
    if (closed.get()) {
      log.debug("Sender is closed, rejecting metric: {}", name);
      return false;
    }
    try {
      Map<String, String> allTags = mergeTags(tags, additionalTags);
      addEnvironmentTags(allTags);

      String metricName = sanitizeMetricName(type + "_" + name);
      Map<String, Object> event = buildMetricEvent(metricName, value, allTags);

      boolean added = metricQueue.offer(event);
      if (added) {
        metricsEnqueuedCount.incrementAndGet();
        log.debug(
            "Enqueued metric: {} = {} (queue size: {})", metricName, value, metricQueue.size());
      } else {
        droppedMetricsCount.incrementAndGet();
        log.warn(
            "Metric queue full, dropping metric: {} = {} (total dropped: {})",
            metricName,
            value,
            droppedMetricsCount.get());
      }
      return added;
    } catch (Exception e) {
      log.warn("Error enqueuing metric: {} = {}", name, value, e);
      return false;
    }
  }

  public boolean sendMetrics(Map<String, Object> metrics, Map<String, String> tags) {
    return sendMetrics(metrics, tags, System.currentTimeMillis());
  }

  public boolean sendMetrics(
      Map<String, Object> metrics, Map<String, String> tags, long timestamp) {
    if (closed.get()) {
      log.debug("Sender is closed, rejecting metrics");
      return false;
    }
    if (metrics == null || metrics.isEmpty()) {
      log.debug("No metrics to send");
      return false;
    }

    try {
      Map<String, String> allTags = new HashMap<>(tags != null ? tags : Map.of());
      addEnvironmentTags(allTags);

      for (Map.Entry<String, Object> metric : metrics.entrySet()) {
        String sanitizedName = sanitizeMetricName(metric.getKey());
        Map<String, Object> event =
            buildMetricEvent(sanitizedName, metric.getValue(), allTags, timestamp);

        boolean added = metricQueue.offer(event);
        if (added) {
          metricsEnqueuedCount.incrementAndGet();
        } else {
          droppedMetricsCount.incrementAndGet();
          log.warn(
              "Metric queue full, dropping metric: {} (total dropped: {})",
              metric.getKey(),
              droppedMetricsCount.get());
        }
      }

      return true;
    } catch (Exception e) {
      log.warn("Error enqueuing batch metrics", e);
      return false;
    }
  }

  public MetricStats getStats() {
    return new MetricStats(
        metricsEnqueuedCount.get(),
        metricsSentCount.get(),
        batchesSentCount.get(),
        droppedMetricsCount.get(),
        totalRetriesCount.get(),
        totalFailuresCount.get(),
        metricQueue.size(),
        queueCapacity);
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      log.debug("Splunk Metric Sender already closed, skipping");
      return;
    }

    log.info(
        "Closing Splunk Metric Sender, {} metrics in queue, {} total dropped",
        metricQueue.size(),
        droppedMetricsCount.get());

    running.set(false);

    workerExecutor.shutdown();
    try {
      if (!workerExecutor.awaitTermination(shutdownTimeoutSeconds, TimeUnit.SECONDS)) {
        log.warn("Worker did not terminate in time, forcing shutdown");
        workerExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      workerExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }

    MetricStats finalStats = getStats();
    log.info(
        "Splunk Metric Sender closed - Stats: enqueued={}, sent={}, batches={}, dropped={}, retries={}, failures={}",
        finalStats.getMetricsEnqueued(),
        finalStats.getMetricsSent(),
        finalStats.getBatchesSent(),
        finalStats.getMetricsDropped(),
        finalStats.getTotalRetries(),
        finalStats.getTotalFailures());
  }

  public boolean flush() {
    if (!running.get()) {
      log.debug("Sender is closed, skipping flush");
      return false;
    }

    CountDownLatch latch = new CountDownLatch(1);
    if (flushLatch.compareAndSet(null, latch)) {
      try {
        boolean completed = latch.await(flushTimeoutSeconds, TimeUnit.SECONDS);
        if (!completed) {
          log.warn("Flush timeout - worker did not complete in time");
          flushLatch.compareAndSet(latch, null);
        }
        return completed;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("Flush interrupted", e);
        flushLatch.compareAndSet(latch, null);
        return false;
      }
    } else {
      log.debug("Flush already in progress, skipping");
      return false;
    }
  }

  private String maskUrl(String url) {
    try {
      URI uri = URI.create(url);
      String scheme = uri.getScheme() != null ? uri.getScheme() : "unknown";
      String host = uri.getHost() != null ? uri.getHost() : "unknown";
      return scheme + "://" + host;
    } catch (Exception e) {
      return "<masked>";
    }
  }

  private void validateConfig(SplunkConfig config) {
    Objects.requireNonNull(config, "SplunkConfig cannot be null");
    Objects.requireNonNull(config.getHecUrl(), "HEC URL is required");
    Objects.requireNonNull(config.getToken(), "Token is required");

    try {
      URI uri = URI.create(config.getHecUrl());
      if (uri.getHost() == null || uri.getHost().isEmpty()) {
        throw new IllegalArgumentException("HEC URL must have a valid host");
      }
      if (!"https".equalsIgnoreCase(uri.getScheme())) {
        log.warn(
            "⚠️  Splunk HEC URL should use HTTPS for security. Current scheme: {}",
            uri.getScheme());
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid HEC URL format: " + e.getMessage(), e);
    }

    if (config.getIndex() == null || config.getIndex().trim().isEmpty()) {
      throw new IllegalArgumentException("Index is required");
    }

    if (config.getToken().trim().isEmpty()) {
      throw new IllegalArgumentException("Token cannot be empty");
    }

    if (config.getBatchSize() <= 0) {
      throw new IllegalArgumentException(
          "Batch size must be positive, got: " + config.getBatchSize());
    }
    if (config.getQueueCapacity() <= 0) {
      throw new IllegalArgumentException(
          "Queue capacity must be positive, got: " + config.getQueueCapacity());
    }
    if (config.getFlushIntervalSeconds() <= 0) {
      throw new IllegalArgumentException(
          "Flush interval must be positive, got: " + config.getFlushIntervalSeconds());
    }
    if (config.getFlushTimeoutSeconds() <= 0) {
      throw new IllegalArgumentException(
          "Flush timeout must be positive, got: " + config.getFlushTimeoutSeconds());
    }
    if (config.getShutdownTimeoutSeconds() <= 0) {
      throw new IllegalArgumentException(
          "Shutdown timeout must be positive, got: " + config.getShutdownTimeoutSeconds());
    }
    if (config.getMaxRetries() < 0) {
      throw new IllegalArgumentException(
          "Max retries cannot be negative, got: " + config.getMaxRetries());
    }
    if (config.getInitialRetryDelayMs() <= 0) {
      throw new IllegalArgumentException(
          "Initial retry delay must be positive, got: " + config.getInitialRetryDelayMs());
    }
    if (config.getMaxRetryDelayMs() <= 0) {
      throw new IllegalArgumentException(
          "Max retry delay must be positive, got: " + config.getMaxRetryDelayMs());
    }
    if (config.getMaxRetryDelayMs() < config.getInitialRetryDelayMs()) {
      throw new IllegalArgumentException(
          "Max retry delay ("
              + config.getMaxRetryDelayMs()
              + "ms) must be >= initial retry delay ("
              + config.getInitialRetryDelayMs()
              + "ms)");
    }
    if (config.getHttpConnectTimeoutSeconds() <= 0) {
      throw new IllegalArgumentException(
          "HTTP connect timeout must be positive, got: " + config.getHttpConnectTimeoutSeconds());
    }
    if (config.getHttpRequestTimeoutSeconds() <= 0) {
      throw new IllegalArgumentException(
          "HTTP request timeout must be positive, got: " + config.getHttpRequestTimeoutSeconds());
    }

    if (config.getBatchSize() > config.getQueueCapacity()) {
      log.warn(
          "Batch size ({}) is larger than queue capacity ({}). This may cause delays.",
          config.getBatchSize(),
          config.getQueueCapacity());
    }
  }

  private String sanitizeMetricName(String metricName) {
    if (metricName == null || metricName.isEmpty()) {
      log.warn("Empty or null metric name provided, using 'unknown'");
      return "unknown";
    }

    String sanitized = metricName.replaceAll("[^a-zA-Z0-9_\\-.:]+", "_");

    sanitized = sanitized.replaceAll("^_+|_+$", "");

    if (sanitized.isEmpty()) {
      log.warn("Metric name '{}' became empty after sanitization, using 'invalid'", metricName);
      return "invalid";
    }

    if (!sanitized.equals(metricName)) {
      log.debug("Metric name sanitized: '{}' -> '{}'", metricName, sanitized);
    }

    return sanitized;
  }

  public long getDroppedMetricsCount() {
    return droppedMetricsCount.get();
  }

  private void sendBatchAsync(List<Map<String, Object>> batch) {
    if (batch.isEmpty()) {
      return;
    }

    int batchSize = batch.size();
    try {
      StringBuilder batchPayload = new StringBuilder();
      for (Map<String, Object> event : batch) {
        batchPayload.append(toJson(event)).append("\n");
      }

      String payload = batchPayload.toString();
      sendBatch(payload);

      metricsSentCount.addAndGet(batchSize);
      batchesSentCount.incrementAndGet();

      log.debug("Sent batch of {} metrics", batchSize);
    } catch (Exception e) {
      totalFailuresCount.incrementAndGet();
      log.warn("Error sending batch of {} metrics", batchSize, e);
    }
  }

  private static HttpClient createDefaultHttpClient(SplunkConfig config) {
    return HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(config.getHttpConnectTimeoutSeconds()))
        .version(HttpClient.Version.HTTP_1_1)
        .build();
  }

  private static ExecutorService createWorkerExecutor() {
    return Executors.newSingleThreadExecutor(
        runnable -> {
          Thread thread = new Thread(runnable, "splunk-metric-sender");
          thread.setDaemon(true);
          return thread;
        });
  }

  private ObjectMapper configureObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setLocale(Locale.US);
    mapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
    return mapper;
  }

  private void startBackgroundWorker() {
    workerExecutor.submit(
        () -> {
          List<Map<String, Object>> batch = new ArrayList<>(batchSize);
          long lastFlushTime = System.currentTimeMillis();

          while (running.get() || !metricQueue.isEmpty()) {
            try {
              Map<String, Object> metric = metricQueue.poll(100, TimeUnit.MILLISECONDS);

              if (metric != null) {
                batch.add(metric);
              }

              // Check if manual flush was requested
              CountDownLatch latch = flushLatch.get();
              if (latch != null) {
                try {
                  if (!batch.isEmpty()) {
                    sendBatchAsync(batch);
                    batch.clear();
                    lastFlushTime = System.currentTimeMillis();
                  }
                } catch (Exception e) {
                  log.error("Flush failed", e);
                } finally {
                  latch.countDown(); // Always signal completion, even on failure
                  flushLatch.compareAndSet(latch, null);
                }
                continue;
              }

              long now = System.currentTimeMillis();
              long timeSinceLastFlush = now - lastFlushTime;
              boolean timeThresholdReached = timeSinceLastFlush >= (flushIntervalSeconds * 1000);

              if (batch.size() >= batchSize || (timeThresholdReached && !batch.isEmpty())) {
                sendBatchAsync(batch);
                batch.clear();
                lastFlushTime = now;
              }

            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              log.warn("Background worker interrupted", e);
              break;
            } catch (Exception e) {
              log.error("Error in background worker", e);
            }
          }

          if (!batch.isEmpty()) {
            sendBatchAsync(batch);
          }

          log.info("Background worker stopped");
        });
  }

  private Map<String, Object> buildMetricEvent(
      String metricName, Object value, Map<String, String> dimensions) {
    return buildMetricEvent(metricName, value, dimensions, System.currentTimeMillis());
  }

  private Map<String, Object> buildMetricEvent(
      String metricName, Object value, Map<String, String> dimensions, long timestampMillis) {
    Map<String, Object> event = new HashMap<>();

    event.put("index", index);
    event.put("source", source);
    event.put("time", BigDecimal.valueOf(timestampMillis / 1000.0));
    event.put("event", "metric");

    Map<String, Object> fields = new HashMap<>();
    fields.put("metric_name:" + metricName, value);

    if (dimensions != null && !dimensions.isEmpty()) {
      fields.putAll(dimensions);
    }

    event.put("fields", fields);
    return event;
  }

  private void sendBatch(String jsonPayload) throws IOException, InterruptedException {
    int payloadSize = jsonPayload.getBytes(StandardCharsets.UTF_8).length;
    if (payloadSize > 800_000) {
      log.warn(
          "Batch payload size {} bytes exceeds 800KB threshold (approaching Splunk 1MB limit). Consider reducing batch size or tag verbosity.",
          payloadSize);
    }

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(hecUrl))
            .header("Authorization", "Splunk " + token)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
            .timeout(Duration.ofSeconds(httpRequestTimeoutSeconds))
            .build();

    Exception lastException = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        HttpResponse<String> response =
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        int statusCode = response.statusCode();
        if (statusCode == 200) {
          log.debug("Splunk HEC response: {}", response.body());
          return;
        } else if (statusCode >= 500) {
          log.warn(
              "Splunk HEC returned server error (attempt {}/{}): {} - {}",
              attempt + 1,
              maxRetries + 1,
              statusCode,
              response.body());
          lastException = new IOException("Server error: " + statusCode + " - " + response.body());
        } else {
          log.warn("Splunk HEC returned client error: {} - {}", statusCode, response.body());
          return;
        }
      } catch (IOException e) {
        log.warn(
            "Network error sending to Splunk HEC (attempt {}/{}): {}",
            attempt + 1,
            maxRetries + 1,
            e.getMessage());
        lastException = e;
      }

      if (attempt < maxRetries) {
        totalRetriesCount.incrementAndGet();

        long baseDelayMs = initialRetryDelayMs * (1L << attempt);
        long cappedBaseDelayMs = Math.min(baseDelayMs, maxRetryDelayMs);
        long delayMs = ThreadLocalRandom.current().nextLong(cappedBaseDelayMs + 1);
        log.debug(
            "Retrying in {} ms (base: {} ms, capped: {} ms)",
            delayMs,
            baseDelayMs,
            cappedBaseDelayMs);
        Thread.sleep(delayMs);
      }
    }

    if (lastException != null) {
      throw new IOException(
          "Failed to send batch after " + (maxRetries + 1) + " attempts", lastException);
    }
  }

  private String toJson(Object obj) throws IOException {
    return objectMapper.writeValueAsString(obj);
  }

  private Map<String, String> mergeTags(
      Map<String, String> tags, Map<String, String> additionalTags) {
    Map<String, String> merged = new HashMap<>(tags != null ? tags : Map.of());
    if (additionalTags != null) {
      merged.putAll(additionalTags);
    }
    return merged;
  }

  private void addEnvironmentTags(Map<String, String> tags) {
    String podName = System.getenv("HOSTNAME");
    String namespace = System.getenv("POD_NAMESPACE");

    if (podName != null && !podName.isEmpty()) {
      tags.put("pod", podName);
    }
    if (namespace != null && !namespace.isEmpty()) {
      tags.put("namespace", namespace);
    }
  }
}
