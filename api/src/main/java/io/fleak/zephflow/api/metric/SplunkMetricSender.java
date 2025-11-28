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
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private static final int QUEUE_CAPACITY = 10000;
  private static final long FLUSH_INTERVAL_SECONDS = 5;

  private final HttpClient httpClient;
  private final String hecUrl;
  private final String token;
  private final String source;
  private final String index;
  private final ObjectMapper objectMapper;
  private final LinkedBlockingQueue<Map<String, Object>> metricQueue;
  private final ExecutorService workerExecutor;
  private final ScheduledExecutorService scheduledExecutor;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final AtomicReference<CountDownLatch> flushLatch = new AtomicReference<>();

  public SplunkMetricSender(SplunkConfig config) {
    this(config, createDefaultHttpClient());
  }

  public SplunkMetricSender(SplunkConfig config, HttpClient httpClient) {
    this.hecUrl = config.getHecUrl();
    this.token = config.getToken();
    this.source = config.getSource() != null ? config.getSource() : "zephflow";
    this.index = config.getIndex();
    this.httpClient = httpClient;
    this.objectMapper = configureObjectMapper();
    this.metricQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    this.workerExecutor = createWorkerExecutor();
    this.scheduledExecutor = createScheduledExecutor();

    startBackgroundWorker();
    startPeriodicFlusher();

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
        log.warn("Metric queue full, dropping metric: {} = {}", metricName, value);
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
        Map<String, Object> event = buildMetricEvent(metric.getKey(), metric.getValue(), allTags);
        event.put("time", BigDecimal.valueOf(timestamp / 1000.0));

        boolean added = metricQueue.offer(event);
        if (!added) {
          log.warn("Metric queue full, dropping metric: {}", metric.getKey());
        }
      }
    } catch (Exception e) {
      log.warn("Error enqueuing batch metrics", e);
    }
  }

  public void flush() {
    CountDownLatch latch = new CountDownLatch(1);
    if (flushLatch.compareAndSet(null, latch)) {
      try {
        boolean completed = latch.await(5, TimeUnit.SECONDS);
        if (!completed) {
          log.warn("Flush timeout - background worker did not complete in time");
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

  @Override
  public void close() {
    log.info("Closing Splunk Metric Sender, {} metrics in queue", metricQueue.size());

    running.set(false);

    scheduledExecutor.shutdown();
    try {
      if (!scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduledExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduledExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }

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

    log.info("Splunk Metric Sender closed");
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

  private static ScheduledExecutorService createScheduledExecutor() {
    return Executors.newSingleThreadScheduledExecutor(
        runnable -> {
          Thread thread = new Thread(runnable, "splunk-metric-flusher");
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

          while (running.get() || !metricQueue.isEmpty()) {
            try {
              Map<String, Object> metric = metricQueue.poll(100, TimeUnit.MILLISECONDS);

              if (metric != null) {
                batch.add(metric);

                if (batch.size() >= BATCH_SIZE) {
                  sendBatchAsync(batch);
                  batch.clear();
                }
              }

              CountDownLatch latch = flushLatch.get();
              if (latch != null) {
                try {
                  if (!batch.isEmpty()) {
                    sendBatchAsync(batch);
                    batch.clear();
                  }
                  latch.countDown();
                } catch (Exception e) {
                  log.error("Flush failed, not signaling completion", e);
                } finally {
                  flushLatch.compareAndSet(latch, null);
                }
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

  private void startPeriodicFlusher() {
    scheduledExecutor.scheduleAtFixedRate(
        () -> {
          try {
            flushPending();
          } catch (Exception e) {
            log.error("Error in periodic flusher", e);
          }
        },
        FLUSH_INTERVAL_SECONDS,
        FLUSH_INTERVAL_SECONDS,
        TimeUnit.SECONDS);
  }

  private void flushPending() {
    List<Map<String, Object>> batch = new ArrayList<>();
    metricQueue.drainTo(batch, BATCH_SIZE);

    if (!batch.isEmpty()) {
      log.debug("Periodic flush: {} metrics", batch.size());
      sendBatchAsync(batch);
    }
  }

  private Map<String, Object> buildMetricEvent(
      String metricName, Object value, Map<String, String> dimensions) {
    Map<String, Object> event = new HashMap<>();

    event.put("index", index);
    event.put("source", source);
    event.put("time", BigDecimal.valueOf(System.currentTimeMillis() / 1000.0));
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

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      log.warn(
          "Splunk HEC returned non-200 status: {} - {}", response.statusCode(), response.body());
    } else {
      log.debug("Splunk HEC response: {}", response.body());
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
