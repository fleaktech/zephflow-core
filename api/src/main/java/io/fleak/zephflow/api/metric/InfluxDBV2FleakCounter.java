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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBV2FleakCounter implements FleakCounter {

  private final String name;
  private final Map<String, String> tags;
  private final InfluxDBV2MetricSender metricSender;

  private final ConcurrentHashMap<String, PendingIncrement> pendingIncrements =
      new ConcurrentHashMap<>();
  private final int flushThreshold;
  private final long flushIntervalMs;

  public InfluxDBV2FleakCounter(
      String name, Map<String, String> tags, InfluxDBV2MetricSender metricSender) {
    this(name, tags, metricSender, 1000, 1000);
  }

  public InfluxDBV2FleakCounter(
      String name,
      Map<String, String> tags,
      InfluxDBV2MetricSender metricSender,
      int flushThreshold,
      long flushIntervalMs) {
    this.name = name;
    this.tags = tags;
    this.metricSender = metricSender;
    this.flushThreshold = flushThreshold;
    this.flushIntervalMs = flushIntervalMs;
  }

  @Override
  public void increase(Map<String, String> additionalTags) {
    increase(1L, additionalTags);
  }

  @Override
  public void increase(long n, Map<String, String> additionalTags) {
    String tagKey = getTagKey(additionalTags);

    PendingIncrement pending =
        pendingIncrements.computeIfAbsent(tagKey, k -> new PendingIncrement(additionalTags));
    long accumulated = pending.value.addAndGet(n);
    long timeSinceLastFlush = System.currentTimeMillis() - pending.lastFlushTime.get();

    if (accumulated >= flushThreshold || timeSinceLastFlush >= flushIntervalMs) {
      flushTagGroup(pending);
    }
  }

  private void flushTagGroup(PendingIncrement pending) {
    long toSend = pending.value.getAndSet(0);

    if (toSend > 0) {
      pending.lastFlushTime.set(System.currentTimeMillis());
      metricSender.sendMetric("counter", name, toSend, tags, pending.additionalTags);
    }
  }

  public void flush() {
    for (Map.Entry<String, PendingIncrement> entry : pendingIncrements.entrySet()) {
      flushTagGroup(entry.getValue());
    }
  }

  private String getTagKey(Map<String, String> additionalTags) {
    if (additionalTags == null || additionalTags.isEmpty()) {
      return "default";
    }
    return additionalTags.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(e -> e.getKey() + "=" + e.getValue())
        .reduce((a, b) -> a + "," + b)
        .orElse("default");
  }

  private static class PendingIncrement {
    final AtomicLong value = new AtomicLong(0);
    final AtomicLong lastFlushTime = new AtomicLong(System.currentTimeMillis());
    final Map<String, String> additionalTags;

    PendingIncrement(Map<String, String> additionalTags) {
      this.additionalTags = additionalTags;
    }
  }
}
