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
package io.fleak.zephflow.lib.commands.zerobussink;

import com.databricks.zerobus.StreamConfigurationOptions;
import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusProtoStream;
import com.databricks.zerobus.ZerobusSdk;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand.FlushResult;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand.Flusher;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand.PreparedInputEvents;
import io.fleak.zephflow.lib.commands.sink.UnknownSinkCommitStateException;
import io.fleak.zephflow.lib.credentials.DatabricksCredential;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Flushes records to a Databricks Zerobus stream. Opens a single long-lived stream (protobuf or
 * JSON depending on config), pushes each record non-blocking via {@code ingestRecordOffset}, then
 * blocks on the LAST offset of the batch via {@code waitForOffset}.
 *
 * <p>Blocking only on the last offset is correct because Zerobus guarantees ordered, durable
 * delivery per stream: once {@code waitForOffset(last)} returns, every earlier offset on the same
 * stream is durable too. This is the pattern recommended by the Databricks Zerobus SDK.
 *
 * <p>Delivery is at-least-once; on reconnect the SDK re-sends unacknowledged records, so downstream
 * consumers should dedupe if they need exactly-once.
 */
@Slf4j
public class ZerobusSinkFlusher implements Flusher<Map<String, Object>> {

  private final ZerobusSdk sdk;

  // protobuf mode
  private final ZerobusProtoStream protoStream;
  private final Schema avroSchema;
  private final Descriptors.Descriptor protoDescriptor;

  // json mode
  private final ZerobusJsonStream jsonStream;

  // The Zerobus SDK documents that ZerobusSdk and the stream classes are NOT thread-safe and must
  // be used from a single thread. We hold one long-lived stream per execution context, so serialize
  // all stream/SDK access (incl. close) to prevent interleaved ingests or a close racing a flush.
  private final Object streamLock = new Object();

  ZerobusSinkFlusher(
      ZerobusSdk sdk,
      ZerobusProtoStream protoStream,
      Schema avroSchema,
      Descriptors.Descriptor protoDescriptor,
      ZerobusJsonStream jsonStream) {
    this.sdk = sdk;
    this.protoStream = protoStream;
    this.avroSchema = avroSchema;
    this.protoDescriptor = protoDescriptor;
    this.jsonStream = jsonStream;
  }

  /** Opens the appropriate stream and constructs a ready-to-use flusher. */
  static ZerobusSinkFlusher create(ZerobusSinkDto.Config config, DatabricksCredential credential) {
    ZerobusSdk sdk = ZerobusClientFactory.createClient(config.getZerobusEndpoint(), credential);
    // Use the SDK's default stream configuration (incl. its own in-flight defaults); we don't
    // override tuning knobs the user never configured.
    StreamConfigurationOptions options = StreamConfigurationOptions.getDefault();
    try {
      if (ZerobusSinkDto.ENCODING_JSON.equalsIgnoreCase(config.getEncodingType())) {
        ZerobusJsonStream jsonStream =
            sdk.createJsonStream(
                    config.getTableName(),
                    credential.getClientId(),
                    credential.getClientSecret(),
                    options)
                .join();
        return new ZerobusSinkFlusher(sdk, null, null, null, jsonStream);
      }

      Schema avroSchema = AvroToProtoDescriptorConverter.parseAvro(config.getAvroSchema());
      var descriptorProto =
          AvroToProtoDescriptorConverter.toDescriptorProto(
              config.getAvroSchema(), config.getTableName());
      Descriptors.Descriptor descriptor =
          AvroToProtoDescriptorConverter.toDescriptor(descriptorProto);
      ZerobusProtoStream protoStream =
          sdk.createProtoStream(
                  config.getTableName(),
                  descriptorProto,
                  credential.getClientId(),
                  credential.getClientSecret(),
                  options)
              .join();
      return new ZerobusSinkFlusher(sdk, protoStream, avroSchema, descriptor, null);
    } catch (RuntimeException e) {
      // stream creation failed: clean up the SDK before surfacing
      try {
        sdk.close();
      } catch (RuntimeException closeError) {
        log.warn("Failed to close Zerobus SDK after stream creation failure", closeError);
      }
      throw e;
    }
  }

  @Override
  public FlushResult flush(
      PreparedInputEvents<Map<String, Object>> preparedInputEvents, Map<String, String> metricTags)
      throws Exception {
    // Encode every payload BEFORE touching the stream. A per-record encoding failure becomes an
    // ErrorOutput for that record only; it must not fail the whole batch (which would happen if we
    // threw after some records had already been enqueued — the Flusher contract says a thrown
    // exception means nothing was written, which would no longer be true).
    List<ErrorOutput> errors = new ArrayList<>();
    long flushedDataSize = 0;

    synchronized (streamLock) {
      if (jsonStream != null) {
        List<String> payloads = new ArrayList<>();
        for (Pair<RecordFleakData, Map<String, Object>> pair :
            preparedInputEvents.rawAndPreparedList()) {
          try {
            String json = JsonUtils.OBJECT_MAPPER.writeValueAsString(pair.getRight());
            payloads.add(json);
            flushedDataSize += json.getBytes(StandardCharsets.UTF_8).length;
          } catch (Exception e) {
            errors.add(
                new ErrorOutput(pair.getLeft(), "Zerobus JSON encode failed: " + e.getMessage()));
          }
        }
        Optional<Long> offset =
            ingestWithUnknownCommitState(
                !payloads.isEmpty(), () -> jsonStream.ingestRecordsOffset(payloads));
        awaitDurability(offset, jsonStream::waitForOffset);
        return new FlushResult(payloads.size(), flushedDataSize, errors);
      }

      List<byte[]> payloads = new ArrayList<>();
      for (Pair<RecordFleakData, Map<String, Object>> pair :
          preparedInputEvents.rawAndPreparedList()) {
        try {
          DynamicMessage message =
              AvroToProtoDescriptorConverter.toDynamicMessage(
                  pair.getRight(), avroSchema, protoDescriptor);
          byte[] bytes = message.toByteArray();
          payloads.add(bytes);
          flushedDataSize += bytes.length;
        } catch (Exception e) {
          errors.add(
              new ErrorOutput(pair.getLeft(), "Zerobus protobuf encode failed: " + e.getMessage()));
        }
      }
      Optional<Long> offset =
          ingestWithUnknownCommitState(
              !payloads.isEmpty(), () -> protoStream.ingestRecordsOffset(payloads));
      awaitDurability(offset, protoStream::waitForOffset);
      return new FlushResult(payloads.size(), flushedDataSize, errors);
    }
  }

  /**
   * Ingests a non-empty batch, mapping any failure to {@link UnknownSinkCommitStateException}. Once
   * {@code ingestRecordsOffset} crosses into the SDK/native layer, records may already have been
   * enqueued or committed even if the call then throws — so a failure here means the commit state
   * is unknown, exactly like a durability-wait failure. Treating it as an ordinary exception would
   * let {@link io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand} convert the batch into
   * per-record failures (silently dropped in the non-DLQ path). An empty batch never touches the
   * SDK.
   */
  private Optional<Long> ingestWithUnknownCommitState(
      boolean hasPayloads, OffsetIngestor ingestor) {
    if (!hasPayloads) {
      return Optional.empty();
    }
    try {
      return ingestor.ingest();
    } catch (Exception e) {
      throw new UnknownSinkCommitStateException(
          "Zerobus ingest failed after a non-empty batch was handed to the SDK; "
              + "commit state is unknown",
          e);
    }
  }

  @FunctionalInterface
  private interface OffsetIngestor {
    Optional<Long> ingest() throws Exception;
  }

  /**
   * Blocks until the last enqueued offset is durable. If the wait fails after records were already
   * handed to Zerobus, the commit state is unknown: the records may or may not have been written.
   * Throw {@link UnknownSinkCommitStateException} so the runner fails the node instead of reporting
   * a clean per-record failure (which, in the non-DLQ path, would silently drop records that may
   * actually be committed).
   */
  private void awaitDurability(Optional<Long> offset, OffsetWaiter waiter) {
    if (offset.isEmpty()) {
      return;
    }
    try {
      waiter.waitForOffset(offset.get());
    } catch (Exception e) {
      throw new UnknownSinkCommitStateException(
          "Zerobus accepted records but durability confirmation failed; commit state is unknown — "
              + "not treating this batch as retryable per-record failures",
          e);
    }
  }

  @FunctionalInterface
  private interface OffsetWaiter {
    void waitForOffset(long offset) throws Exception;
  }

  @Override
  public void close() {
    synchronized (streamLock) {
      try {
        if (protoStream != null) {
          protoStream.close();
        }
        if (jsonStream != null) {
          jsonStream.close();
        }
      } catch (Exception e) {
        log.warn("Error closing Zerobus stream", e);
      } finally {
        try {
          sdk.close();
        } catch (RuntimeException e) {
          log.warn("Error closing Zerobus SDK", e);
        }
      }
    }
  }
}
