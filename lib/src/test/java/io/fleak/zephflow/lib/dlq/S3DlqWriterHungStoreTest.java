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
package io.fleak.zephflow.lib.dlq;

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_DLQ_RECORDS_NOT_WRITTEN;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class S3DlqWriterHungStoreTest {

  private final FleakCounter unwrittenDeadLetterCounter = mock(FleakCounter.class);

  @Test
  void givesUpATenMegabyteBatchOnAStoreThatNeverAnswersWithinNinetySeconds() throws Exception {
    try (HungS3Server store = new HungS3Server();
        DlqWriter writer = writer(store.endpoint(), 1)) {
      writer.open();
      long elapsedMillis =
          assertTimeoutPreemptively(
              Duration.ofSeconds(150),
              () -> {
                long start = System.nanoTime();
                writer.writeToDlq(1L, event(new byte[10_000_000]), "error", "node-1");
                return Duration.ofNanos(System.nanoTime() - start).toMillis();
              });

      assertTrue(elapsedMillis < 90_000, "gave up after " + elapsedMillis + " ms");
      verify(unwrittenDeadLetterCounter).increase(1, Map.of());
    }
  }

  @Test
  void closeCountsBufferedDeadLettersAsNotWrittenOnceItsDeadlinePasses() throws Exception {
    try (HungS3Server store = new HungS3Server()) {
      DlqWriter writer = writer(store.endpoint(), 100);
      writer.open();
      writer.writeToDlq(1L, event("a".getBytes()), "error", "node-1");
      writer.writeToDlq(2L, event("b".getBytes()), "error", "node-1");

      long elapsedMillis =
          assertTimeoutPreemptively(
              Duration.ofSeconds(90),
              () -> {
                long start = System.nanoTime();
                writer.close();
                return Duration.ofNanos(System.nanoTime() - start).toMillis();
              });

      assertTrue(elapsedMillis < 40_000, "closed after " + elapsedMillis + " ms");
      verify(unwrittenDeadLetterCounter).increase(2, Map.of());
    }
  }

  @Test
  void interruptStopsRetryingKeepsTheFlagAndCountsTheBatch() throws Exception {
    DlqWriter writer = writer(HungS3Server.refusingEndpoint(), 1);
    writer.open();
    AtomicBoolean interruptedAfterWrite = new AtomicBoolean();
    Thread pipeline =
        new Thread(
            () -> {
              writer.writeToDlq(1L, event("a".getBytes()), "error", "node-1");
              interruptedAfterWrite.set(Thread.currentThread().isInterrupted());
            });
    pipeline.start();
    Thread.sleep(300);

    pipeline.interrupt();
    pipeline.join(2_000);

    assertFalse(pipeline.isAlive(), "kept retrying after the interrupt");
    assertTrue(interruptedAfterWrite.get());
    verify(unwrittenDeadLetterCounter).increase(1, Map.of());
    writer.close();
  }

  private DlqWriter writer(String endpoint, int batchSize) {
    MetricClientProvider metricClientProvider = mock(MetricClientProvider.class);
    when(metricClientProvider.counter(METRIC_NAME_DLQ_RECORDS_NOT_WRITTEN, Map.of()))
        .thenReturn(unwrittenDeadLetterCounter);
    JobContext.S3DlqConfig config =
        JobContext.S3DlqConfig.builder()
            .region("us-east-1")
            .bucket("dlq")
            .batchSize(batchSize)
            .flushIntervalMillis(600_000)
            .accessKeyId("access-" + UUID.randomUUID())
            .secretAccessKey("x-" + UUID.randomUUID())
            .s3EndpointOverride(endpoint)
            .build();
    return DlqWriterFactory.createDlqWriter(config, "prefix", metricClientProvider, Map.of());
  }

  private static SerializedEvent event(byte[] value) {
    return new SerializedEvent(null, value, Map.of());
  }
}
