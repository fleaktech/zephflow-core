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
package io.fleak.zephflow.lib.commands.kafkasink;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Batching Kafka sink flusher that accumulates records across multiple flush calls and sends them
 * in larger batches to improve throughput. Adapted from BatchS3Commiter pattern.
 */
@Slf4j
public class BatchKafkaSinkFlusher implements SimpleSinkCommand.Flusher<RecordFleakData> {

  private final KafkaProducer<byte[], byte[]> producer;
  private final String topic;
  private final FleakSerializer<?> fleakSerializer;
  private final PathExpression partitionKeyExpression;
  private final int batchSize;

  // Thread-safe buffer for accumulating records across flush calls
  private final List<RecordFleakData> buffer = new ArrayList<>();
  private final Object bufferLock = new Object();

  private final ScheduledFuture<?> scheduledFuture;
  private volatile boolean closed = false;

  public BatchKafkaSinkFlusher(
      KafkaProducer<byte[], byte[]> producer,
      String topic,
      FleakSerializer<?> fleakSerializer,
      PathExpression partitionKeyExpression,
      int batchSize,
      long flushIntervalMillis,
      ScheduledExecutorService scheduler) {
    this.producer = producer;
    this.topic = topic;
    this.fleakSerializer = fleakSerializer;
    this.partitionKeyExpression = partitionKeyExpression;
    this.batchSize = batchSize;

    // Start the scheduled flush task
    this.scheduledFuture =
        scheduler.scheduleAtFixedRate(
            this::flushBufferScheduled,
            flushIntervalMillis,
            flushIntervalMillis,
            TimeUnit.MILLISECONDS);
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents) throws Exception {

    if (closed) {
      throw new IllegalStateException("BatchKafkaSinkFlusher is closed");
    }

    List<RecordFleakData> events = preparedInputEvents.preparedList();
    if (events.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    synchronized (bufferLock) {
      buffer.addAll(events);

      // Trigger immediate flush if buffer size threshold is reached
      if (buffer.size() >= batchSize) {
        return flushBufferToKafka();
      }
    }

    // Return empty result since no actual flushing occurred yet
    return new SimpleSinkCommand.FlushResult(0, 0, List.of());
  }

  /** Scheduled flush task that runs periodically to flush buffered records */
  private void flushBufferScheduled() {
    try {
      synchronized (bufferLock) {
        if (!buffer.isEmpty()) {
          flushBufferToKafka();
        }
      }
    } catch (Exception e) {
      log.error("Error during scheduled flush", e);
    }
  }

  /**
   * Flushes the current buffer contents to Kafka. Must be called while holding bufferLock.
   *
   * @return FlushResult with statistics from the flush operation
   */
  private SimpleSinkCommand.FlushResult flushBufferToKafka() {
    if (buffer.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    // Create a copy of buffer and clear original
    List<RecordFleakData> batchToFlush = new ArrayList<>(buffer);
    buffer.clear();

    log.debug("Flushing batch of {} records to Kafka topic {}", batchToFlush.size(), topic);

    List<ErrorOutput> errorOutputs = new ArrayList<>();
    long flushedDataSize = 0;
    int totalSends = 0;

    // Use CountDownLatch to wait for all async callbacks to complete
    CountDownLatch latch = new CountDownLatch(batchToFlush.size());
    AtomicInteger asyncErrors = new AtomicInteger(0);

    // Send each record in the batch to Kafka
    for (RecordFleakData event : batchToFlush) {
      try {
        var serializedEvent = fleakSerializer.serialize(List.of(event));
        byte[] keyBytesValue = null;

        if (partitionKeyExpression != null) {
          String keyValue = partitionKeyExpression.getStringValueFromEventOrDefault(event, null);
          if (keyValue != null) {
            keyBytesValue = keyValue.getBytes(StandardCharsets.UTF_8);
          }
        }

        var eventValue = serializedEvent.value();

        if (eventValue != null && eventValue.length > 0) {
          // Send to Kafka with callback for sync error handling
          producer.send(
              new ProducerRecord<>(topic, keyBytesValue, eventValue),
              (metadata, exception) -> {
                try {
                  if (exception != null) {
                    log.error(
                        "Kafka producer failed to send event: {}", toJsonString(event), exception);
                    // Safe to add directly since only one thread calls flush()
                    errorOutputs.add(new ErrorOutput(event, exception.getMessage()));
                    asyncErrors.incrementAndGet();
                  } else {
                    log.debug(
                        "Sent event to Kafka: topic={}, partition={}, offset={}",
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset());
                  }
                } finally {
                  latch.countDown();
                }
              });
          flushedDataSize += eventValue.length;
          totalSends++;
        } else {
          // No data to send, count down immediately
          latch.countDown();
        }
      } catch (Exception e) {
        // This catches serialization errors or other immediate issues
        log.error("Failed to send Kafka record for event: {}", toJsonString(event), e);
        errorOutputs.add(new ErrorOutput(event, e.getMessage()));
        latch.countDown(); // Count down even for sync errors
      }
    }

    // Wait for all async operations to complete before returning
    try {
      if (!latch.await(30, TimeUnit.SECONDS)) {
        log.warn(
            "Timeout waiting for Kafka producer callbacks to complete for batch of {} records",
            batchToFlush.size());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Interrupted while waiting for Kafka producer callbacks", e);
    }

    int successfulSends = totalSends - errorOutputs.size();

    log.debug(
        "Batch flush completed: {} successful, {} errors, {} bytes",
        successfulSends,
        errorOutputs.size(),
        flushedDataSize);

    return new SimpleSinkCommand.FlushResult(successfulSends, flushedDataSize, errorOutputs);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    closed = true;

    // Cancel the scheduled flush task
    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);
    }

    // Flush any remaining records in the buffer
    synchronized (bufferLock) {
      if (!buffer.isEmpty()) {
        log.info("Flushing {} remaining records during close", buffer.size());
        flushBufferToKafka();
      }
    }

    // Close the Kafka producer
    if (producer != null) {
      producer.close();
    }

    log.info("BatchKafkaSinkFlusher closed successfully");
  }
}
