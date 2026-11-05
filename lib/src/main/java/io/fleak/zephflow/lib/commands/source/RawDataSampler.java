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
package io.fleak.zephflow.lib.commands.source;

import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.random.RandomGenerator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawDataSampler<T> implements Closeable {

  static final int SAMPLES_PER_INTERVAL = 10;

  static final RandomGenerator THREAD_LOCAL_RANDOM = () -> ThreadLocalRandom.current().nextLong();

  private final RawDataEncoder<T> encoder;
  private final DlqWriter sampleWriter;
  private final String nodeId;
  private final long intervalMs;
  private final int perInterval;
  private final RandomGenerator random;
  private final AtomicInteger remaining;
  private ScheduledExecutorService scheduler;

  RawDataSampler(
      RawDataEncoder<T> encoder,
      DlqWriter sampleWriter,
      String nodeId,
      long intervalMs,
      int perInterval,
      RandomGenerator random) {
    this.encoder = encoder;
    this.sampleWriter = sampleWriter;
    this.nodeId = nodeId;
    this.intervalMs = intervalMs;
    this.perInterval = perInterval;
    this.random = random;
    this.remaining = new AtomicInteger(perInterval);
  }

  public void open() {
    sampleWriter.open();
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "raw-data-sampler-timer");
              t.setDaemon(true);
              return t;
            });
    scheduler.scheduleAtFixedRate(
        () -> remaining.set(perInterval), intervalMs, intervalMs, TimeUnit.MILLISECONDS);
  }

  public void maybeSample(List<T> batch) {
    if (batch.isEmpty()) {
      return;
    }
    int take = Math.min(remaining.getAndUpdate(n -> Math.max(0, n - batch.size())), batch.size());
    if (take == 0) {
      return;
    }
    for (int index : distinctRandomIndexes(batch.size(), take)) {
      try {
        SerializedEvent serializedEvent = encoder.serialize(batch.get(index));
        sampleWriter.writeToDlq(System.currentTimeMillis(), serializedEvent, "", nodeId);
      } catch (Exception e) {
        log.debug("failed to sample raw data", e);
      }
    }
  }

  private int[] distinctRandomIndexes(int size, int count) {
    int[] indexes = new int[size];
    for (int i = 0; i < size; i++) {
      indexes[i] = i;
    }
    for (int i = 0; i < count; i++) {
      int j = i + random.nextInt(size - i);
      int swap = indexes[i];
      indexes[i] = indexes[j];
      indexes[j] = swap;
    }
    return Arrays.copyOf(indexes, count);
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    try {
      sampleWriter.close();
    } catch (Exception e) {
      log.debug("failed to close sample writer", e);
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> RawDataSampler<T> noOp() {
    return (RawDataSampler<T>) NoOpHolder.INSTANCE;
  }

  private static class NoOpHolder {
    static final RawDataSampler<?> INSTANCE = new NoOpRawDataSampler<>();
  }

  private static class NoOpRawDataSampler<T> extends RawDataSampler<T> {
    NoOpRawDataSampler() {
      super(null, null, null, 0, 0, null);
    }

    @Override
    public void open() {}

    @Override
    public void maybeSample(List<T> batch) {}

    @Override
    public void close() {}
  }
}
