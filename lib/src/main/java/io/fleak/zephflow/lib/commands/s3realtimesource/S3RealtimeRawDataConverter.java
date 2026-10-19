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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.NodeExecutionException;
import io.fleak.zephflow.lib.commands.s3realtimesource.S3ObjectDecoder.Result;
import io.fleak.zephflow.lib.commands.source.BatchEmittingConverter;
import io.fleak.zephflow.lib.commands.source.DeadLetterWriteException;
import io.fleak.zephflow.lib.commands.source.RawDataEncoder;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Decodes the S3 objects an {@link S3EventMessage} names, one at a time, and emits each object's
 * records in bounded batches while it is decoded. A bad record that carries at most 128 KiB is
 * dead-lettered with its own bytes; every other failure is summarized once per object and kind, and
 * downstream failures once per object and failing node, with the notification as the value. The
 * message's receipt handle is queued for deletion only when it is acknowledged.
 */
@Slf4j
public class S3RealtimeRawDataConverter implements BatchEmittingConverter<S3EventMessage> {

  static final String S3_METADATA_BUCKET = "__s3_bucket";
  static final String S3_METADATA_KEY = "__s3_key";
  static final int MAX_RECORD_ENTRY_BYTES = 128 * 1024;
  static final int MAX_BATCH_RECORDS = 1000;
  static final int MAX_LISTED_RANGES = 100;
  private static final long VALUE_BYTES = 8;
  private static final String UNKNOWN_NODE = "unknown";

  private final S3RegionalClientProvider s3Clients;
  private final S3ObjectDecoder decoder;
  private final String format;
  private final long maxObjectSizeBytes;
  private final Path spoolDir;
  private final long batchBytes;
  private final Queue<String> confirmedReceiptHandles;
  private final DlqWriter dlqWriter;
  private final RawDataEncoder<S3EventMessage> encoder;
  private final String nodeId;
  private final FleakCounter skippedObjectCounter;

  public S3RealtimeRawDataConverter(
      S3RegionalClientProvider s3Clients,
      S3ObjectDecoder decoder,
      String format,
      long maxObjectSizeBytes,
      Path spoolDir,
      long batchBytes,
      Queue<String> confirmedReceiptHandles,
      DlqWriter dlqWriter,
      RawDataEncoder<S3EventMessage> encoder,
      String nodeId,
      FleakCounter skippedObjectCounter) {
    this.s3Clients = s3Clients;
    this.decoder = decoder;
    this.format = format;
    this.maxObjectSizeBytes = maxObjectSizeBytes;
    this.spoolDir = spoolDir;
    this.batchBytes = batchBytes;
    this.confirmedReceiptHandles = confirmedReceiptHandles;
    this.dlqWriter = dlqWriter;
    this.encoder = encoder;
    this.nodeId = nodeId;
    this.skippedObjectCounter = skippedObjectCounter;
  }

  @Override
  public void emit(S3EventMessage message, SourceExecutionContext<?> context, Emitter emitter) {
    for (S3ObjectRef ref : message.objectRefs()) {
      if (emitter.stopped()) {
        return;
      }
      new ObjectRun(message, ref, context, emitter).run();
    }
  }

  @Override
  public void acknowledge(S3EventMessage message) {
    confirmedReceiptHandles.add(message.receiptHandle());
  }

  /** The record-size estimate batches are capped by: 8 bytes per value and field, plus text. */
  static long estimate(FleakData data) {
    if (data instanceof RecordFleakData record) {
      long size = VALUE_BYTES;
      for (Map.Entry<String, FleakData> field : record.getPayload().entrySet()) {
        size += VALUE_BYTES + field.getKey().length() + estimate(field.getValue());
      }
      return size;
    }
    if (data instanceof ArrayFleakData array) {
      long size = VALUE_BYTES;
      for (FleakData item : array.getArrayPayload()) {
        size += estimate(item);
      }
      return size;
    }
    if (data instanceof StringPrimitiveFleakData string) {
      return VALUE_BYTES + string.getStringValue().length();
    }
    return VALUE_BYTES;
  }

  /** The simple name of the innermost cause that is not a SourceReadException, with an AWS code. */
  static String causeName(Throwable error) {
    Throwable chosen = error;
    for (Throwable t : ExceptionUtils.getThrowableList(error)) {
      if (!(t instanceof SourceReadException)) {
        chosen = t;
      }
    }
    String name = chosen.getClass().getSimpleName();
    if (chosen instanceof AwsServiceException aws
        && aws.awsErrorDetails() != null
        && aws.awsErrorDetails().errorCode() != null) {
      name += " " + aws.awsErrorDetails().errorCode();
    }
    return name;
  }

  /**
   * Record index ranges, adjacent ones merged, with the bytes they span when known. Only the first
   * {@link #MAX_LISTED_RANGES} are kept; the rest are counted. A record index under 1 (a whole
   * document) is shown as "the document".
   */
  static final class IndexRanges {
    private final List<long[]> ranges = new ArrayList<>(); // first, last, spanStart, spanEnd
    private long lastIndex = Long.MIN_VALUE;
    private long dropped;

    void add(long recordIndex, RawSpan span) {
      add(recordIndex, recordIndex, span);
    }

    /** Adds the records {@code first} to {@code last}; a record index under 1 is a document. */
    void add(long first, long last, RawSpan span) {
      long start = span != null && span.length() >= 0 ? span.offset() : -1;
      long end = start >= 0 ? span.offset() + span.length() : -1;
      boolean adjacent = first >= 1 && lastIndex + 1 == first;
      lastIndex = last;
      if (adjacent && dropped == 0) {
        long[] previous = ranges.getLast();
        previous[1] = last;
        if (start >= 0) {
          previous[2] = previous[2] < 0 ? start : Math.min(previous[2], start);
          previous[3] = Math.max(previous[3], end);
        }
      } else if (adjacent) {
        return;
      } else if (ranges.size() < MAX_LISTED_RANGES) {
        ranges.add(new long[] {first, last, start, end});
      } else {
        dropped++;
      }
    }

    boolean isEmpty() {
      return ranges.isEmpty();
    }

    /** "recordIndex 1-3 (bytes 0-99), 7 (bytes 200-250)", or "the document (bytes 0-12)". */
    String describe() {
      StringBuilder out = new StringBuilder();
      boolean indexed = false;
      for (int i = 0; i < ranges.size(); i++) {
        long[] r = ranges.get(i);
        if (i > 0) {
          out.append(", ");
        }
        if (r[0] < 1) {
          out.append("the document");
        } else {
          if (!indexed) {
            out.append("recordIndex ");
            indexed = true;
          }
          out.append(r[0] == r[1] ? Long.toString(r[0]) : r[0] + "-" + r[1]);
        }
        if (r[2] >= 0) {
          out.append(" (bytes ").append(r[2]).append('-').append(r[3]).append(')');
        }
      }
      if (dropped > 0) {
        out.append(", and ").append(dropped).append(" more ranges");
      }
      return out.toString();
    }
  }

  /** The failures of one kind in one object, summarized into one DLQ entry. */
  private static final class Summary {
    final String firstLine;
    final String firstDetail;
    final IndexRanges ranges = new IndexRanges();
    long count;
    boolean ended;
    long endedAt;

    Summary(String firstLine, String firstDetail) {
      this.firstLine = firstLine;
      this.firstDetail = firstDetail;
    }
  }

  /** The downstream failures at one node for one object. */
  private static final class Downstream {
    final String stackTrace;
    final IndexRanges ranges = new IndexRanges();
    long batches;

    Downstream(String stackTrace) {
      this.stackTrace = stackTrace;
    }
  }

  /** Decoding one object: its batch, its summaries, its counters. */
  private final class ObjectRun {
    private final S3EventMessage message;
    private final S3ObjectRef ref;
    private final SourceExecutionContext<?> context;
    private final Emitter emitter;
    private final Map<String, Summary> summaries = new LinkedHashMap<>();
    private final Map<String, Downstream> downstream = new LinkedHashMap<>();
    private List<RecordFleakData> batch = new ArrayList<>();
    private long batchSize;
    private long batchFirstIndex = -1;
    private long batchLastIndex = -1;
    private long lastSpanEnd = -1;
    private long decodedCount;
    private RecordFleakData firstRecord;
    private String versionId;

    ObjectRun(
        S3EventMessage message,
        S3ObjectRef ref,
        SourceExecutionContext<?> context,
        Emitter emitter) {
      this.message = message;
      this.ref = ref;
      this.context = context;
      this.emitter = emitter;
    }

    void run() {
      Result result;
      try {
        S3Client client = s3Clients.clientFor(ref.region());
        result = decoder.decode(client, ref, maxObjectSizeBytes, spoolDir, this::visit);
      } catch (DeadLetterWriteException e) {
        throw e;
      } catch (Exception e) {
        if (emitter.stopped()) {
          return;
        }
        flush();
        if (emitter.stopped()) {
          return;
        }
        context.deserializeFailureCounter().increase(Map.of());
        summary("READ_FAILURE s3 object: " + causeName(e), ExceptionUtils.getStackTrace(e)).count++;
        finish(-1);
        return;
      }
      versionId = result.versionId();
      switch (result.status()) {
        case MISSING -> {
          skippedObjectCounter.increase(Map.of("reason", "missing"));
          log.warn(
              "S3 object s3://{}/{} no longer exists; skipping the notification",
              ref.bucket(),
              ref.key());
        }
        case TOO_LARGE -> {
          skippedObjectCounter.increase(Map.of("reason", "oversized"));
          writeNotification(
              "OBJECT_TOO_LARGE s3 object\n"
                  + objectName()
                  + " size "
                  + result.storedLength()
                  + " exceeds maxObjectSizeBytes "
                  + maxObjectSizeBytes,
              nodeId);
        }
        case STOPPED -> {}
        case FINISHED -> {
          if (emitter.stopped()) {
            return;
          }
          flush();
          if (!emitter.stopped()) {
            finish(result.storedLength());
          }
        }
      }
    }

    private boolean visit(DecodeOutcome outcome) {
      if (emitter.stopped()) {
        return false;
      }
      switch (outcome) {
        case DecodeOutcome.Decoded decoded -> {
          track(decoded.span());
          if (batch.isEmpty()) {
            batchFirstIndex = decoded.recordIndex();
          }
          batchLastIndex = decoded.recordIndex();
          batch.add(decoded.record());
          batchSize += estimate(decoded.record());
          if (firstRecord == null) {
            firstRecord = decoded.record();
          }
          decodedCount++;
          if (batch.size() >= MAX_BATCH_RECORDS || batchSize >= batchBytes) {
            flush();
          }
        }
        case DecodeOutcome.Failed failed -> {
          track(failed.span());
          context.deserializeFailureCounter().increase(Map.of());
          if (!failed.terminal()
              && failed.raw().length > 0
              && failed.raw().length <= MAX_RECORD_ENTRY_BYTES) {
            writeRecordEntry(failed);
          } else {
            Summary summary =
                summary(
                    failed.kind() + " " + format + " object: " + causeName(failed.error()),
                    ExceptionUtils.getStackTrace(failed.error()));
            summary.count++;
            summary.ranges.add(failed.recordIndex(), failed.span());
            if (failed.terminal()) {
              summary.ended = true;
              summary.endedAt = failed.recordIndex();
            }
          }
        }
        case DecodeOutcome.Oversized oversized -> {
          track(oversized.span());
          context.deserializeFailureCounter().increase(Map.of());
          String limit =
              "over the limit (maxRecordBytes "
                  + decoder.limits().maxRecordBytes()
                  + ", maxDocumentBytes "
                  + decoder.limits().maxDocumentBytes()
                  + ")";
          Summary summary =
              summary(
                  "OVERSIZED " + format + " object",
                  limit + ", reached " + oversized.sizeSoFar() + " bytes");
          summary.count++;
          summary.ranges.add(oversized.recordIndex(), oversized.span());
          if (oversized.terminal()) {
            summary.ended = true;
            summary.endedAt = oversized.recordIndex();
          }
        }
      }
      return !emitter.stopped();
    }

    private void track(RawSpan span) {
      if (span != null && span.length() >= 0) {
        lastSpanEnd = Math.max(lastSpanEnd, span.offset() + span.length());
      }
    }

    private void flush() {
      if (batch.isEmpty()) {
        return;
      }
      List<RecordFleakData> emitting = batch;
      long first = batchFirstIndex;
      long last = batchLastIndex;
      batch = new ArrayList<>();
      batchSize = 0;
      context
          .inputEventCounter()
          .increase(emitting.size(), getCallingUserTagAndEventTags(null, emitting.getFirst()));
      try {
        emitter.accept(emitting);
      } catch (Exception e) {
        // After a stop the summaries are never written, so this one is dropped with them.
        String failingNode =
            e instanceof NodeExecutionException nee ? nee.getNodeId() : UNKNOWN_NODE;
        Downstream failure =
            downstream.computeIfAbsent(
                failingNode, n -> new Downstream(ExceptionUtils.getStackTrace(e)));
        failure.batches++;
        failure.ranges.add(first, last, null);
      }
    }

    private Summary summary(String firstLine, String detail) {
      return summaries.computeIfAbsent(
          firstLine.substring(0, firstLine.indexOf(' ')), k -> new Summary(firstLine, detail));
    }

    private void finish(long storedLength) {
      long size = decodedCount == 0 ? 0 : lastSpanEnd >= 0 ? lastSpanEnd : storedLength;
      if (size > 0) {
        context.dataSizeCounter().increase(size, getCallingUserTagAndEventTags(null, firstRecord));
      }
      for (Summary summary : summaries.values()) {
        StringBuilder text =
            new StringBuilder(summary.firstLine)
                .append('\n')
                .append(summary.firstDetail)
                .append('\n')
                .append(objectName())
                .append("; ")
                .append(summary.count)
                .append(summary.count == 1 ? " outcome" : " outcomes");
        if (!summary.ranges.isEmpty()) {
          text.append("; ").append(summary.ranges.describe());
        }
        if (summary.ended) {
          text.append("; the object ended");
          if (summary.endedAt >= 1) {
            text.append(" at recordIndex ").append(summary.endedAt);
          }
        }
        writeNotification(text.toString(), nodeId);
      }
      for (Map.Entry<String, Downstream> entry : downstream.entrySet()) {
        Downstream failure = entry.getValue();
        String text =
            failure.stackTrace
                + "\n"
                + objectName()
                + "; "
                + failure.batches
                + (failure.batches == 1 ? " batch" : " batches")
                + " failed; "
                + failure.ranges.describe();
        writeNotification(text, entry.getKey());
      }
    }

    private void writeRecordEntry(DecodeOutcome.Failed failed) {
      StringBuilder identity = new StringBuilder(objectName());
      if (failed.recordIndex() >= 1) {
        identity.append(", recordIndex ").append(failed.recordIndex());
      }
      if (failed.span() != null && failed.span().length() >= 0) {
        identity
            .append(", bytes ")
            .append(failed.span().offset())
            .append('-')
            .append(failed.span().offset() + failed.span().length());
      }
      String text =
          failed.kind()
              + " "
              + format
              + " record: "
              + causeName(failed.error())
              + "\n"
              + ExceptionUtils.getStackTrace(failed.error())
              + "\n"
              + identity;
      SerializedEvent notification = encoder.serialize(message);
      write(new SerializedEvent(notification.key(), failed.raw(), null), text, nodeId);
    }

    private void writeNotification(String text, String entryNodeId) {
      write(encoder.serialize(message), text, entryNodeId);
    }

    private void write(SerializedEvent value, String text, String entryNodeId) {
      if (dlqWriter == null) {
        log.warn(
            "dropping a failure of s3://{}/{} (no DLQ configured): {}",
            ref.bucket(),
            ref.key(),
            text.lines().findFirst().orElse(""));
        return;
      }
      try {
        dlqWriter.writeToDlq(System.currentTimeMillis(), value, text, entryNodeId);
      } catch (RuntimeException e) {
        if (emitter.stopped()) {
          return;
        }
        throw new DeadLetterWriteException(
            "failed to write a DLQ entry for s3://" + ref.bucket() + "/" + ref.key(), e);
      }
    }

    private String objectName() {
      return "object s3://"
          + ref.bucket()
          + "/"
          + ref.key()
          + (versionId == null ? "" : " version " + versionId);
    }
  }
}
