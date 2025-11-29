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
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SplunkMetricSender implements AutoCloseable {

  @Data
  public static class SplunkConfig {
    private String hecUrl;
    private String token;
    private String source;
    private String index;
  }

  private static final int BATCH_SIZE = 100;
  private static final int QUEUE_CAPACITY = 1000;
  private static final long FLUSH_INTERVAL_SECONDS = 5;
  private static final int MAX_RETRIES = 3;
  private static final long INITIAL_RETRY_DELAY_MS = 100;

  private final HttpClient httpClient;
  private final String hecUrl;
  private final String token;
  private final String source;
  private final String index;
  private final ObjectMapper objectMapper;
  private final LinkedBlockingQueue<Map<String, Object>> metricQueue;
  private final ExecutorService workerExecutor;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final AtomicReference<CountDownLatch> flushLatch = new AtomicReference<>();
  private final AtomicLong droppedMetricsCount = new AtomicLong(0);
  private final Random random = new SecureRandom();

  public SplunkMetricSender(SplunkConfig config) {
    this(config, createDefaultHttpClient());
  }

  public SplunkMetricSender(SplunkConfig config, HttpClient httpClient) {
    Objects.requireNonNull(config, "SplunkConfig cannot be null");
    Objects.requireNonNull(config.getHecUrl(), "HEC URL is required");
    Objects.requireNonNull(config.getToken(), "Token is required");

    this.hecUrl = config.getHecUrl();
    this.token = config.getToken();
    this.source = config.getSource() != null ? config.getSource() : "zephflow";
    this.index = config.getIndex();
    this.httpClient = httpClient;
    this.objectMapper = configureObjectMapper();
    this.metricQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    this.workerExecutor = createWorkerExecutor();

    startBackgroundWorker();

    log.info("Splunk Metric Sender initialized with HEC URL: {}", hecUrl);
  }

  public void sendMetric(
      String type,
      String name,
      Object value,
      Map<String, String> tags,
      Map<String, String> additionalTags) {
    try {
      Map<String, String> allTags = mergeTags(tags, additionalTags);
      addEnvironmentTags(allTags);

      String metricName = type + "_" + name;
      Map<String, Object> event = buildMetricEvent(metricName, value, allTags);

      boolean added = metricQueue.offer(event);
      if (added) {
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
    } catch (Exception e) {
      log.warn("Error enqueuing metric: {} = {}", name, value, e);
    }
  }

  public void sendMetrics(Map<String, Object> metrics, Map<String, String> tags) {
    sendMetrics(metrics, tags, System.currentTimeMillis());
  }

  public void sendMetrics(Map<String, Object> metrics, Map<String, String> tags, long timestamp) {
    if (metrics == null || metrics.isEmpty()) {
      log.debug("No metrics to send");
      return;
    }

    try {
      Map<String, String> allTags = new HashMap<>(tags != null ? tags : Map.of());
      addEnvironmentTags(allTags);

      for (Map.Entry<String, Object> metric : metrics.entrySet()) {
        Map<String, Object> event =
            buildMetricEvent(metric.getKey(), metric.getValue(), allTags, timestamp);

        boolean added = metricQueue.offer(event);
        if (!added) {
          droppedMetricsCount.incrementAndGet();
          log.warn(
              "Metric queue full, dropping metric: {} (total dropped: {})",
              metric.getKey(),
              droppedMetricsCount.get());
        }
      }
    } catch (Exception e) {
      log.warn("Error enqueuing batch metrics", e);
    }
  }

  public void flush() {
    if (!running.get()) {
      log.debug("Sender is closed, skipping flush");
      return;
    }

    CountDownLatch latch = new CountDownLatch(1);
    if (flushLatch.compareAndSet(null, latch)) {
      try {
        boolean completed = latch.await(10, TimeUnit.SECONDS);
        if (!completed) {
          log.warn("Flush timeout - worker did not complete in time");
          flushLatch.compareAndSet(latch, null);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.warn("Flush interrupted", e);
        flushLatch.compareAndSet(latch, null);
      }
    } else {
      log.debug("Flush already in progress, skipping");
    }
  }

  public long getDroppedMetricsCount() {
    return droppedMetricsCount.get();
  }

  @Override
  public void close() {
    log.info(
        "Closing Splunk Metric Sender, {} metrics in queue, {} total dropped",
        metricQueue.size(),
        droppedMetricsCount.get());

    running.set(false);

    workerExecutor.shutdown();
    try {
      if (!workerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        log.warn("Worker did not terminate in time, forcing shutdown");
        workerExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      workerExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }

    log.info("Splunk Metric Sender closed, {} total metrics dropped", droppedMetricsCount.get());
  }

  private void sendBatchAsync(List<Map<String, Object>> batch) {
    if (batch.isEmpty()) {
      return;
    }

    try {
      StringBuilder batchPayload = new StringBuilder();
      for (Map<String, Object> event : batch) {
        batchPayload.append(toJson(event)).append("\n");
      }

      String payload = batchPayload.toString();
      sendBatch(payload);

      log.debug("Sent batch of {} metrics", batch.size());
    } catch (Exception e) {
      log.warn("Error sending batch of {} metrics", batch.size(), e);
    }
  }

  private static HttpClient createDefaultHttpClient() {
    return HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
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
          List<Map<String, Object>> batch = new ArrayList<>(BATCH_SIZE);
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
                  latch.countDown();
                } catch (Exception e) {
                  log.error("Flush failed, not signaling completion", e);
                } finally {
                  flushLatch.compareAndSet(latch, null);
                }
                continue;
              }

              long now = System.currentTimeMillis();
              long timeSinceLastFlush = now - lastFlushTime;
              boolean timeThresholdReached = timeSinceLastFlush >= (FLUSH_INTERVAL_SECONDS * 1000);

              if (batch.size() >= BATCH_SIZE || (timeThresholdReached && !batch.isEmpty())) {
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
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(hecUrl))
            .header("Authorization", "Splunk " + token)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
            .timeout(Duration.ofSeconds(30))
            .build();

    Exception lastException = null;
    for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
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
              MAX_RETRIES + 1,
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
            MAX_RETRIES + 1,
            e.getMessage());
        lastException = e;
      }

      if (attempt < MAX_RETRIES) {
        // Exponential backoff with full jitter to prevent thundering herd
        long baseDelayMs = INITIAL_RETRY_DELAY_MS * (1L << attempt);
        long delayMs = random.nextLong(baseDelayMs + 1);
        log.debug("Retrying in {} ms (base: {} ms)", delayMs, baseDelayMs);
        Thread.sleep(delayMs);
      }
    }

    if (lastException != null) {
      throw new IOException(
          "Failed to send batch after " + (MAX_RETRIES + 1) + " attempts", lastException);
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
