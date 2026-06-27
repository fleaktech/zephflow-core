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
import io.fleak.zephflow.lib.commands.sink.RetriableConnectionException;
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
 *
 * <p>When constructed with a {@code config} + {@code credential} (the production path), the flusher
 * can rebuild its stream after a connectivity failure: a failed flush marks the stream unhealthy
 * and the next flush reconnects. This is what lets the store-and-forward forwarder replay buffered
 * records after an outage.
 */
@Slf4j
public class ZerobusSinkFlusher implements Flusher<Map<String, Object>> {

  // Fully-qualified table name, carried only so every log line identifies which Zerobus sink it
  // belongs to (a pipeline may have several).
  private final String tableName;

  // Present only on the production path; enables reconnect. Null in unit tests (mocked streams).
  private final ZerobusSinkDto.Config config;
  private final DatabricksCredential credential;

  private ZerobusSdk sdk;

  // protobuf mode
  private ZerobusProtoStream protoStream;
  private Schema avroSchema;
  private Descriptors.Descriptor protoDescriptor;

  // json mode
  private ZerobusJsonStream jsonStream;

  // Set to false when a flush fails; the next flush reconnects (production path only).
  private volatile boolean healthy = true;

  // The Zerobus SDK documents that ZerobusSdk and the stream classes are NOT thread-safe and must
  // be used from a single thread. We hold one long-lived stream per execution context, so serialize
  // all stream/SDK access (incl. close and reconnect) to prevent interleaved ingests.
  private final Object streamLock = new Object();

  ZerobusSinkFlusher(
      String tableName,
      ZerobusSdk sdk,
      ZerobusProtoStream protoStream,
      Schema avroSchema,
      Descriptors.Descriptor protoDescriptor,
      ZerobusJsonStream jsonStream) {
    this.tableName = tableName;
    this.config = null;
    this.credential = null;
    this.sdk = sdk;
    this.protoStream = protoStream;
    this.avroSchema = avroSchema;
    this.protoDescriptor = protoDescriptor;
    this.jsonStream = jsonStream;
  }

  private ZerobusSinkFlusher(ZerobusSinkDto.Config config, DatabricksCredential credential) {
    this.tableName = config.getTableName();
    this.config = config;
    this.credential = credential;
  }

  /** Opens the appropriate stream and constructs a ready-to-use flusher. */
  static ZerobusSinkFlusher create(ZerobusSinkDto.Config config, DatabricksCredential credential) {
    ZerobusSinkFlusher flusher = new ZerobusSinkFlusher(config, credential);
    flusher.connect();
    return flusher;
  }

  /**
   * Opens a fresh SDK + stream from config/credential and installs them. Caller must hold {@link
   * #streamLock} or be the constructor.
   */
  private void connect() {
    ZerobusSdk newSdk = ZerobusClientFactory.createClient(config.getZerobusEndpoint(), credential);
    // Builder seeds from getDefault(), so maxInflightRecords (50_000) and recovery settings stay
    // intact; we only tighten the ack/flush timeouts so an outage is detected in seconds, not the
    // SDK's default minutes (60s ack / 5min flush).
    StreamConfigurationOptions options =
        StreamConfigurationOptions.builder()
            .setServerLackOfAckTimeoutMs(config.getAckTimeoutMillis())
            .setFlushTimeoutMs(config.getFlushTimeoutMillis())
            .build();
    try {
      if (ZerobusSinkDto.ENCODING_JSON.equalsIgnoreCase(config.getEncodingType())) {
        ZerobusJsonStream newJsonStream =
            newSdk
                .createJsonStream(
                    config.getTableName(),
                    credential.getClientId(),
                    credential.getClientSecret(),
                    options)
                .join();
        this.jsonStream = newJsonStream;
        this.protoStream = null;
        this.avroSchema = null;
        this.protoDescriptor = null;
        log.info("Opened Zerobus JSON stream for table {}", tableName);
      } else {
        Schema schema = AvroToProtoDescriptorConverter.parseAvro(config.getAvroSchema());
        var descriptorProto =
            AvroToProtoDescriptorConverter.toDescriptorProto(
                config.getAvroSchema(), config.getTableName());
        Descriptors.Descriptor descriptor =
            AvroToProtoDescriptorConverter.toDescriptor(descriptorProto);
        ZerobusProtoStream newProtoStream =
            newSdk
                .createProtoStream(
                    config.getTableName(),
                    descriptorProto,
                    credential.getClientId(),
                    credential.getClientSecret(),
                    options)
                .join();
        this.protoStream = newProtoStream;
        this.avroSchema = schema;
        this.protoDescriptor = descriptor;
        this.jsonStream = null;
        log.info("Opened Zerobus protobuf stream for table {}", tableName);
      }
      this.sdk = newSdk;
      this.healthy = true;
    } catch (RuntimeException e) {
      // Stream open failed (bad/unsupported Avro schema, descriptor build, auth, or the open
      // itself). Log at error level with context BEFORE surfacing: otherwise the only sink output
      // is the earlier "Creating Zerobus SDK" info line and the real cause never reaches the
      // pipeline log (on the initial path the node just crash-loops on init).
      log.error(
          "Failed to open Zerobus stream for table {} (endpoint {}, encoding {}): {}",
          tableName,
          config.getZerobusEndpoint(),
          config.getEncodingType(),
          e.getMessage(),
          e);
      // stream creation failed: clean up the SDK before surfacing
      try {
        newSdk.close();
      } catch (RuntimeException closeError) {
        log.warn(
            "Failed to close Zerobus SDK after stream creation failure for table {}",
            tableName,
            closeError);
      }
      throw e;
    }
  }

  @Override
  public FlushResult flush(
      PreparedInputEvents<Map<String, Object>> preparedInputEvents, Map<String, String> metricTags)
      throws Exception {
    synchronized (streamLock) {
      ensureConnected();
      try {
        return doFlushLocked(preparedInputEvents);
      } catch (RuntimeException e) {
        // A flush failure may have broken the stream; force a reconnect on the next attempt so the
        // store-and-forward forwarder can recover once the network is back.
        healthy = false;
        throw e;
      }
    }
  }

  /**
   * Reconnects if a previous flush marked the stream unhealthy. Only possible on the production
   * path (config present); a connection failure here means nothing was sent, so it surfaces as a
   * {@link RetriableConnectionException} for the store-and-forward classifier.
   */
  private void ensureConnected() {
    if (healthy || config == null) {
      return;
    }
    log.info("Zerobus stream unhealthy; reconnecting before flush");
    closeStreamsQuietly();
    try {
      connect();
    } catch (RuntimeException e) {
      throw new RetriableConnectionException("Zerobus reconnect failed", e);
    }
  }

  private FlushResult doFlushLocked(PreparedInputEvents<Map<String, Object>> preparedInputEvents) {
    // Encode every payload BEFORE touching the stream. A per-record encoding failure becomes an
    // ErrorOutput for that record only; it must not fail the whole batch (which would happen if we
    // threw after some records had already been enqueued — the Flusher contract says a thrown
    // exception means nothing was written, which would no longer be true).
    List<ErrorOutput> errors = new ArrayList<>();
    long flushedDataSize = 0;

    if (jsonStream != null) {
      List<String> payloads = new ArrayList<>();
      for (Pair<RecordFleakData, Map<String, Object>> pair :
          preparedInputEvents.rawAndPreparedList()) {
        try {
          String json = JsonUtils.OBJECT_MAPPER.writeValueAsString(pair.getRight());
          payloads.add(json);
          flushedDataSize += json.getBytes(StandardCharsets.UTF_8).length;
        } catch (Exception e) {
          // Per-record failures become ErrorOutputs which, in the non-DLQ path, are counted and
          // then discarded — so log each one or the reason never reaches the pipeline log.
          log.warn(
              "Zerobus JSON encode failed for a record to table {}: {}", tableName, e.getMessage());
          errors.add(
              new ErrorOutput(pair.getLeft(), "Zerobus JSON encode failed: " + e.getMessage()));
        }
      }
      Optional<Long> offset =
          ingestWithUnknownCommitState(
              !payloads.isEmpty(), () -> jsonStream.ingestRecordsOffset(payloads));
      awaitDurability(offset, jsonStream::waitForOffset);
      log.info(
          "Zerobus flush to table {} (json): {} records committed, {} failed, {} bytes, offset {}",
          tableName,
          payloads.size(),
          errors.size(),
          flushedDataSize,
          offset.map(String::valueOf).orElse("none"));
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
        // Per-record failures become ErrorOutputs which, in the non-DLQ path, are counted and
        // then discarded — so log each one or the reason never reaches the pipeline log. This is
        // where a record/schema mismatch (e.g. an unexpected field) surfaces.
        log.warn(
            "Zerobus protobuf encode failed for a record to table {}: {}",
            tableName,
            e.getMessage());
        errors.add(
            new ErrorOutput(pair.getLeft(), "Zerobus protobuf encode failed: " + e.getMessage()));
      }
    }
    Optional<Long> offset =
        ingestWithUnknownCommitState(
            !payloads.isEmpty(), () -> protoStream.ingestRecordsOffset(payloads));
    awaitDurability(offset, protoStream::waitForOffset);
    log.info(
        "Zerobus flush to table {} (protobuf): {} records committed, {} failed, {} bytes, offset {}",
        tableName,
        payloads.size(),
        errors.size(),
        flushedDataSize,
        offset.map(String::valueOf).orElse("none"));
    return new FlushResult(payloads.size(), flushedDataSize, errors);
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
      log.error(
          "Zerobus ingest failed for table {} after a non-empty batch was handed to the SDK; "
              + "commit state is unknown: {}",
          tableName,
          e.getMessage(),
          e);
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
      log.error(
          "Zerobus durability wait failed for table {} at offset {}; commit state is unknown: {}",
          tableName,
          offset.get(),
          e.getMessage(),
          e);
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

  private void closeStreamsQuietly() {
    try {
      if (protoStream != null) {
        protoStream.close();
      }
      if (jsonStream != null) {
        jsonStream.close();
      }
    } catch (Exception e) {
      log.warn("Error closing Zerobus stream for table {}", tableName, e);
    } finally {
      try {
        if (sdk != null) {
          sdk.close();
        }
      } catch (RuntimeException e) {
        log.warn("Error closing Zerobus SDK for table {}", tableName, e);
      }
    }
  }

  @Override
  public void close() {
    synchronized (streamLock) {
      closeStreamsQuietly();
    }
  }
}
