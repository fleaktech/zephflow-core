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
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.ConnectionFailureClassifier;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.encode.RecordEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

/**
 * Kafka sink flusher with two delivery modes. In <b>synchronous</b> mode (the default, {@code
 * deliveryMode: WAIT_FOR_ACK}; always used by store-and-forward) it sends the batch, flushes the
 * producer, and waits for every broker acknowledgement before returning, because the source
 * checkpoint advances as soon as the flush returns. With store-and-forward it additionally
 * <b>throws</b> a classified connectivity failure so {@link SimpleSinkCommand} can buffer the batch
 * locally and replay it once the broker is reachable again. In <b>fire-and-forget</b> mode ({@code
 * deliveryMode: FIRE_AND_FORGET}) it returns as soon as records are in the producer's client-side
 * buffer and reports delivery via async callbacks. In <b>deferred-confirmation</b> mode ({@code
 * WAIT_FOR_ACK} under a source that calls the commit barrier) it sends like fire-and-forget and
 * {@link #awaitDeliveryConfirmation} waits for the acks before the source commits.
 */
@Slf4j
public class KafkaSinkFlusher implements SimpleSinkCommand.Flusher<RecordFleakData> {
  private static final String NO_RECORD_ERROR = "Serializer produced no record";

  private final KafkaProducer<byte[], byte[]> producer;
  private final String topic;
  private final RecordEncoder recordEncoder;
  private final PathExpression partitionKeyExpression;
  private final FleakCounter asyncDeliveredCountCounter;
  private final FleakCounter asyncDeliveredSizeCounter;
  private final FleakCounter asyncErrorCounter;

  private final boolean synchronousDelivery;
  private final boolean deferredConfirmation;
  private final ConnectionFailureClassifier connectionFailureClassifier;
  private final AtomicReference<Exception> undeliveredFailure = new AtomicReference<>();

  // Mid-batch connectivity failures are rethrown only when store-and-forward can buffer and
  // replay the whole batch; without that, records whose acks already succeeded would be
  // misreported as failed.
  private final boolean bufferBatchOnConnectivityFailure;

  private volatile boolean closed = false;

  public KafkaSinkFlusher(
      @NonNull KafkaProducer<byte[], byte[]> producer,
      @NonNull String topic,
      @NonNull RecordEncoder recordEncoder,
      PathExpression partitionKeyExpression, // This one can stay nullable
      @NonNull FleakCounter asyncDeliveredCountCounter,
      @NonNull FleakCounter asyncDeliveredSizeCounter,
      @NonNull FleakCounter asyncErrorCounter) {
    this(
        producer,
        topic,
        recordEncoder,
        partitionKeyExpression,
        asyncDeliveredCountCounter,
        asyncDeliveredSizeCounter,
        asyncErrorCounter,
        false,
        false,
        null,
        false);
  }

  public KafkaSinkFlusher(
      @NonNull KafkaProducer<byte[], byte[]> producer,
      @NonNull String topic,
      @NonNull RecordEncoder recordEncoder,
      PathExpression partitionKeyExpression,
      @NonNull FleakCounter asyncDeliveredCountCounter,
      @NonNull FleakCounter asyncDeliveredSizeCounter,
      @NonNull FleakCounter asyncErrorCounter,
      boolean synchronousDelivery,
      boolean deferredConfirmation,
      ConnectionFailureClassifier connectionFailureClassifier,
      boolean bufferBatchOnConnectivityFailure) {
    this.producer = producer;
    this.topic = topic;
    this.recordEncoder = recordEncoder;
    this.partitionKeyExpression = partitionKeyExpression;
    this.asyncDeliveredCountCounter = asyncDeliveredCountCounter;
    this.asyncDeliveredSizeCounter = asyncDeliveredSizeCounter;
    this.asyncErrorCounter = asyncErrorCounter;
    this.synchronousDelivery = synchronousDelivery;
    this.deferredConfirmation = deferredConfirmation;
    this.connectionFailureClassifier = connectionFailureClassifier;
    this.bufferBatchOnConnectivityFailure = bufferBatchOnConnectivityFailure;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {

    if (closed) {
      throw new IllegalStateException("KafkaSinkFlusher is closed");
    }

    List<RecordFleakData> events = preparedInputEvents.preparedList();
    if (events.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    if (synchronousDelivery) {
      return flushSynchronously(events, metricTags);
    }
    return deferredConfirmation
        ? flushDeferringConfirmation(events, metricTags)
        : flushFireAndForget(events, metricTags);
  }

  private SimpleSinkCommand.FlushResult flushDeferringConfirmation(
      List<RecordFleakData> events, Map<String, String> metricTags) {
    ensureBrokerReachable();

    List<ErrorOutput> errorOutputs = new ArrayList<>();
    int sentCount = 0;
    for (RecordFleakData event : events) {
      try {
        byte[] eventValue = serializeValue(event);
        if (eventValue == null) {
          errorOutputs.add(new ErrorOutput(event, NO_RECORD_ERROR));
          continue;
        }
        PendingAck ack = new PendingAck(eventValue.length, metricTags);
        producer.send(new ProducerRecord<>(topic, keyBytes(event), eventValue), ack);
        Exception rejection = ack.rejectionDuringSend();
        if (rejection != null) {
          errorOutputs.add(new ErrorOutput(event, rejection.getMessage()));
          continue;
        }
        sentCount++;
      } catch (Exception e) {
        errorOutputs.add(new ErrorOutput(event, e.getMessage()));
      }
    }
    return new SimpleSinkCommand.FlushResult(sentCount, 0, errorOutputs);
  }

  /**
   * Waits for every deferred send. A connectivity failure means a record may be lost, so it is
   * thrown to keep the source from committing; other failures were already counted as sink errors.
   */
  @Override
  public void awaitDeliveryConfirmation() {
    if (!deferredConfirmation) {
      return;
    }
    producer.flush();
    Exception failure = undeliveredFailure.getAndSet(null);
    if (failure != null) {
      throw new IllegalStateException(
          "Kafka did not acknowledge records sent to topic " + topic + ": " + failure.getMessage(),
          failure);
    }
  }

  private SimpleSinkCommand.FlushResult flushFireAndForget(
      List<RecordFleakData> events, Map<String, String> metricTags) {
    List<ErrorOutput> errorOutputs = new ArrayList<>();
    int sentCount = 0;

    for (RecordFleakData event : events) {
      try {
        byte[] eventValue = serializeValue(event);
        if (eventValue == null) {
          errorOutputs.add(new ErrorOutput(event, NO_RECORD_ERROR));
          continue;
        }
        byte[] keyBytesValue = keyBytes(event);
        producer.send(
            new ProducerRecord<>(topic, keyBytesValue, eventValue),
            (metadata, exception) -> {
              if (exception != null) {
                log.error(
                    "Kafka producer failed to send event: {}", toJsonString(event), exception);
                asyncErrorCounter.increase(metricTags);
                return;
              }
              asyncDeliveredCountCounter.increase(metricTags);
              asyncDeliveredSizeCounter.increase(eventValue.length, metricTags);
            });
        sentCount++;
      } catch (Exception e) {
        log.error("Failed to send Kafka record for event: {}", toJsonString(event), e);
        errorOutputs.add(new ErrorOutput(event, e.getMessage()));
      }
    }
    return new SimpleSinkCommand.FlushResult(sentCount, 0, errorOutputs);
  }

  /**
   * Sends the batch and waits for acks. A connectivity failure is rethrown so the whole batch is
   * buffered by store-and-forward; a permanent per-record failure stays an {@link ErrorOutput}.
   */
  private SimpleSinkCommand.FlushResult flushSynchronously(
      List<RecordFleakData> events, Map<String, String> metricTags) throws Exception {
    // One bounded metadata probe up front. If the broker is unreachable it throws a connectivity
    // failure after a single wait, buffering the whole batch at once. Without it, on a cold start
    // each send() below would separately block max.block.ms waiting for metadata (N x the cost).
    ensureBrokerReachable();

    List<ErrorOutput> errorOutputs = new ArrayList<>();
    List<Inflight> inflight = new ArrayList<>();

    for (RecordFleakData event : events) {
      try {
        byte[] eventValue = serializeValue(event);
        if (eventValue == null) {
          errorOutputs.add(new ErrorOutput(event, NO_RECORD_ERROR));
          continue;
        }
        Future<RecordMetadata> future =
            producer.send(new ProducerRecord<>(topic, keyBytes(event), eventValue));
        inflight.add(new Inflight(event, eventValue.length, future));
      } catch (Exception e) {
        if (bufferBatchOnConnectivityFailure) {
          rethrowIfConnectivity(e);
        }
        errorOutputs.add(new ErrorOutput(event, e.getMessage()));
      }
    }

    producer.flush(); // ensure every send has been attempted before we wait on the acks

    long deliveredSize = 0;
    int delivered = 0;
    for (Inflight item : inflight) {
      try {
        item.future.get();
        delivered++;
        deliveredSize += item.size;
        asyncDeliveredCountCounter.increase(metricTags);
        asyncDeliveredSizeCounter.increase(item.size, metricTags);
      } catch (ExecutionException e) {
        if (bufferBatchOnConnectivityFailure) {
          rethrowIfConnectivity(e.getCause());
        }
        errorOutputs.add(new ErrorOutput(item.event, e.getCause().getMessage()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      }
    }
    return new SimpleSinkCommand.FlushResult(delivered, deliveredSize, errorOutputs);
  }

  /**
   * Single bounded check that the broker is reachable (metadata for the topic is obtainable). If it
   * is not, throw the connectivity failure so the caller buffers the whole batch after one wait
   * rather than paying {@code max.block.ms} on every per-record {@code send()}.
   */
  private void ensureBrokerReachable() {
    try {
      List<PartitionInfo> partitions = producer.partitionsFor(topic);
      if (partitions == null || partitions.isEmpty()) {
        throw new org.apache.kafka.common.errors.TimeoutException(
            "No partition metadata available for topic " + topic);
      }
    } catch (Exception e) {
      rethrowIfConnectivity(e);
      // Not a connectivity failure: let the per-record send loop surface it as needed.
    }
  }

  private void rethrowIfConnectivity(Throwable t) {
    if (connectionFailureClassifier != null && connectionFailureClassifier.isConnectionFailure(t)) {
      if (t instanceof RuntimeException re) {
        throw re;
      }
      throw new RuntimeException(t);
    }
  }

  private byte[] serializeValue(RecordFleakData event) throws Exception {
    byte[] eventValue = recordEncoder.encode(event);
    return (eventValue == null || eventValue.length == 0) ? null : eventValue;
  }

  private byte[] keyBytes(RecordFleakData event) {
    if (partitionKeyExpression == null) {
      return null;
    }
    String keyValue = partitionKeyExpression.getStringValueFromEventOrDefault(event, null);
    return keyValue == null ? null : keyValue.getBytes(StandardCharsets.UTF_8);
  }

  private boolean bounded;

  @Override
  public SimpleSinkCommand.FlushResult flushBounded(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> input,
      Map<String, String> tags,
      io.fleak.zephflow.api.execution.ExecutionHooks hooks) {
    bounded = true;
    if (closed) throw new IllegalStateException("KafkaSinkFlusher is closed");
    int acknowledged = 0;
    int attempted = 0;
    int rejected = 0;
    long bytes = 0;
    List<ErrorOutput> errors = new ArrayList<>();
    boolean interrupted = false;
    Throwable stopped = null;
    try {
      for (RecordFleakData event : input.preparedList()) {
        hooks.control().checkpoint();
        byte[] value;
        byte[] key;
        try {
          value = serializeValue(event);
          key = keyBytes(event);
          if (value == null) {
            rejected++;
            errors.add(new ErrorOutput(event, NO_RECORD_ERROR));
            continue;
          }
        } catch (Exception failure) {
          rejected++;
          errors.add(new ErrorOutput(event, failure.getMessage()));
          continue;
        }
        attempted++;
        try {
          Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, key, value));
          while (true) {
            try {
              future.get();
              break;
            } catch (InterruptedException interruption) {
              interrupted = true;
            }
          }
          acknowledged++;
          bytes += value.length;
          asyncDeliveredCountCounter.increase(tags);
          asyncDeliveredSizeCounter.increase(value.length, tags);
        } catch (Exception failure) {
          errors.add(new ErrorOutput(event, failure.getMessage()));
        }
      }
    } catch (io.fleak.zephflow.api.execution.ExecutionStoppedException failure) {
      stopped = failure;
    } finally {
      if (interrupted) Thread.currentThread().interrupt();
    }
    var outcome =
        io.fleak.zephflow.api.execution.EffectOutcome.counted(
            (long) attempted,
            (long) acknowledged,
            (long) rejected,
            (long) attempted - acknowledged,
            (long) input.preparedList().size() - attempted,
            "broker_ack");
    var result = new SimpleSinkCommand.FlushResult(acknowledged, bytes, errors, outcome);
    if (stopped != null)
      throw new io.fleak.zephflow.lib.commands.sink.BoundedFlushException(result, stopped);
    return result;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    // Bound shutdown: a wedged producer (e.g. broker unreachable) must not hang terminate()
    // indefinitely while it retries to flush undelivered records.
    if (bounded) producer.close();
    else producer.close(Duration.ofSeconds(10));
    log.info("KafkaSinkFlusher closed successfully");
  }

  /**
   * A failure that completes the send before {@code send()} returns is the caller's to report as an
   * error output; one that arrives later is reported here. {@link #settled} decides which.
   */
  private final class PendingAck implements Callback {
    private final AtomicBoolean settled = new AtomicBoolean();
    private final int size;
    private final Map<String, String> metricTags;
    private volatile Exception rejection;

    PendingAck(int size, Map<String, String> metricTags) {
      this.size = size;
      this.metricTags = metricTags;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception == null) {
        asyncDeliveredCountCounter.increase(metricTags);
        asyncDeliveredSizeCounter.increase(size, metricTags);
        return;
      }
      rejection = exception;
      if (settled.compareAndSet(false, true)) {
        return;
      }
      log.error("Kafka broker did not acknowledge a record sent to topic {}", topic, exception);
      asyncErrorCounter.increase(metricTags);
      asyncErrorCounter.recordErrorSample(exception.getMessage());
      if (connectionFailureClassifier != null
          && connectionFailureClassifier.isConnectionFailure(exception)) {
        undeliveredFailure.compareAndSet(null, exception);
      }
    }

    Exception rejectionDuringSend() {
      return settled.compareAndSet(false, true) ? null : rejection;
    }
  }

  private record Inflight(RecordFleakData event, int size, Future<RecordMetadata> future) {}
}
