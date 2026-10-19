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
import io.fleak.zephflow.lib.commands.source.RawDataEncoder;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import io.fleak.zephflow.lib.serdes.decode.compress.Decompressors;
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
  private static final String RETRY = "then re-upload the object or send this notification again";

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

  /** The words after "OVERSIZED <format> object: ", the detail line, and what to do. */
  record OversizedText(String label, String detail, String action) {}

  /**
   * What an oversized outcome went over and what the operator can do. This source sets the record,
   * document and codec limits from the task's heap ({@link S3DecodeLimits}), so more memory raises
   * them; zstd's window and the format maximum cannot be raised. {@code heapBytes} is 0 when the
   * limits are not shares of a known heap.
   */
  static OversizedText oversizedText(
      String format, boolean multiline, long heapBytes, DecodeOutcome.Oversized oversized) {
    long value = oversized.limitValue();
    long size = oversized.sizeSoFar();
    return switch (oversized.limit()) {
      case MAX_RECORD_BYTES ->
          new OversizedText(
              "maxRecordBytes",
              (multiline ? "a multiline event" : "a record")
                  + " reached "
                  + size
                  + " bytes, over maxRecordBytes "
                  + value,
              multiline
                  ? "check the decoding's multiline startPattern or continuePattern (a pattern"
                      + " that does not match joins events until a limit); if events really are"
                      + " this large, "
                      + moreMemory("maxRecordBytes", value, heapBytes, "event")
                      + "; "
                      + RETRY
                  : moreMemory("maxRecordBytes", value, heapBytes, "record")
                      + ", or write smaller records; if records should be much smaller, check"
                      + " that the source's format matches the object (a wrong format or framing,"
                      + " or an unclosed csv quote, joins many records into one); "
                      + RETRY);
      case MAX_DOCUMENT_BYTES ->
          new OversizedText(
              "maxDocumentBytes",
              "a document or block reached " + size + " bytes, over maxDocumentBytes " + value,
              moreMemory("maxDocumentBytes", value, heapBytes, "document or block")
                  + ", or write smaller documents or avro blocks; if one document holds many"
                  + " records, decode it in a mode that reads them one at a time (text line, json"
                  + " lines or array, xml elements, yaml multi_document, protobuf varint_delimited,"
                  + " avro ocf; as encodingType: STRING_LINE, JSON_OBJECT_LINE or JSON_ARRAY); "
                  + RETRY);
      case MAX_CODEC_MEMORY_BYTES ->
          new OversizedText(
              "xz memory",
              "xz needs " + size + " bytes of memory, over its limit of " + value,
              xzMemory(value)
                  + "recompress the object with a smaller xz dictionary (the presets -0 to -9"
                  + " need at most 65 MiB)"
                  + xzFixed(value)
                  + "; "
                  + RETRY);
      case AVRO_XZ_MEMORY ->
          new OversizedText(
              "avro xz memory",
              "an avro block's xz data needs "
                  + size
                  + " bytes of memory, over its limit of "
                  + value,
              xzMemory(value)
                  + "rewrite the avro file with a lower xz level for its blocks (levels 0 to 9"
                  + " need at most 65 MiB)"
                  + xzFixed(value)
                  + "; "
                  + RETRY);
      case HEAP_SHARE ->
          new OversizedText(
              "parquet heap share",
              "parquet needs " + size + " bytes, over its share of the heap, " + value,
              "give the task more memory, or write the file with smaller row groups or rows"
                  + " (fewer row groups or columns if the footer is too large); "
                  + RETRY);
      case MAX_LINES ->
          new OversizedText(
              "maxLines",
              "a multiline event has more than " + value + " lines (" + size + " bytes)",
              "raise maxLines in the source's decoding, or fix its multiline pattern if events"
                  + " run together; "
                  + RETRY);
      case ZSTD_WINDOW ->
          new OversizedText(
              "zstd window",
              "zstd data needs a window over " + value + " bytes",
              "compress the zstd data (the object, or an avro file's zstandard blocks) without"
                  + " long mode, or with a window of 128 MiB or less (--long=27); no setting"
                  + " raises this limit; "
                  + RETRY);
      case NATIVE_MEMORY ->
          new OversizedText(
              "zstd native memory",
              "zstd could not allocate native memory",
              "the worker ran out of memory outside the Java heap: give the worker more memory"
                  + " or run fewer tasks on it; if it was momentary, "
                  + RETRY);
      case FORMAT_MAXIMUM ->
          new OversizedText(
              "format maximum",
              unit(format)
                  + " reached "
                  + size
                  + " bytes, over the "
                  + value
                  + " bytes the decoder can hold in one piece",
              "write the file in smaller pieces (avro: smaller blocks; protobuf: smaller"
                  + " messages; parquet: a smaller footer); no setting raises this limit; "
                  + RETRY);
    };
  }

  /** "maxRecordBytes is 1/128 of the task's heap, so give the task a heap of at least ...". */
  private static String moreMemory(String setting, long value, long heapBytes, String unit) {
    long share = heapBytes > 0 && value > 0 ? Math.round((double) heapBytes / value) : 0;
    return share > 0
        ? "this source sets "
            + setting
            + " to 1/"
            + share
            + " of the task's heap, so give the task a heap of at least "
            + share
            + " times the "
            + unit
            + "'s size"
        : "this source sets " + setting + " from the task's heap, so give the task more memory";
  }

  private static String xzFixed(long value) {
    return value < Decompressors.XZ_DEFAULT_MEMORY ? "" : "; no setting raises this limit";
  }

  private static String xzMemory(long value) {
    return value < Decompressors.XZ_DEFAULT_MEMORY
        ? "give the task more memory (xz may use half the heap, up to 128 MiB), or "
        : "";
  }

  private static String unit(String format) {
    return switch (format) {
      case "avro" -> "an avro block";
      case "protobuf" -> "a protobuf message";
      case "parquet" -> "the parquet footer";
      default -> "a unit";
    };
  }

  /** What to do about a failure that is not over a limit. */
  static String failureAction(String format, ErrorKind kind) {
    return switch (kind) {
      case MALFORMED ->
          "check that the object is "
              + format
              + " as the source's encodingType or decoding says, or fix the listed records where"
              + " the object is written; "
              + RETRY;
      case CORRUPT_COMPRESSION ->
          "check the source's compression setting against the object, and that the object was"
              + " complete when it was announced; otherwise its compressed data (for avro, possibly"
              + " a block's own codec data) is damaged or has bytes after its end, so rewrite the"
              + " object; "
              + RETRY;
    };
  }

  /**
   * Record index ranges, adjacent ones merged, with the bytes they span when known. Only the first
   * {@link #MAX_LISTED_RANGES} are kept; the rest are counted. Record index 0 (a csv header) is
   * shown as "the header", and -1 (a whole document) as "the document".
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
          out.append(r[0] == 0 ? "the header" : "the document");
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
    final String action;
    final String firstDetail;
    final IndexRanges ranges = new IndexRanges();
    long count;
    boolean ended;
    long endedAt;
    long endedAfter;

    Summary(String firstLine, String action, String firstDetail) {
      this.firstLine = firstLine;
      this.action = action;
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
    private long lastRecordIndex;
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
      } catch (Exception e) {
        if (emitter.stopped()) {
          return;
        }
        flush();
        if (emitter.stopped()) {
          return;
        }
        context.deserializeFailureCounter().increase(Map.of());
        Summary summary =
            summary(
                "READ_FAILURE",
                "READ_FAILURE s3 object: " + causeName(e),
                "check the cause below, such as the source's S3 permissions (s3:GetObject), the"
                    + " network, or free disk where the task runs; "
                    + RETRY,
                ExceptionUtils.getStackTrace(e));
        summary.count++;
        summary.ended = true;
        summary.endedAfter = lastRecordIndex;
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
              "OBJECT_TOO_LARGE s3 object\nAction: raise maxObjectSizeBytes above the object's"
                  + " size, with that much free disk for the task and a visibilityTimeoutSeconds"
                  + " long enough to download it (lower maxNumberOfMessages if several arrive"
                  + " together); "
                  + RETRY
                  + "\n"
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
      if (outcome.recordCount() > 0) {
        lastRecordIndex =
            Math.max(lastRecordIndex, outcome.recordIndex() + outcome.recordCount() - 1);
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
            add(
                summary(
                    failed.kind().name(),
                    failed.kind() + " " + format + " object: " + causeName(failed.error()),
                    failureAction(format, failed.kind()),
                    ExceptionUtils.getStackTrace(failed.error())),
                failed);
          }
        }
        case DecodeOutcome.Oversized oversized -> {
          track(oversized.span());
          context.deserializeFailureCounter().increase(Map.of());
          OversizedText text =
              oversizedText(format, decoder.multiline(), decoder.heapBytes(), oversized);
          add(
              summary(
                  "OVERSIZED " + oversized.limit(),
                  "OVERSIZED " + format + " object: " + text.label(),
                  text.action(),
                  text.detail()),
              oversized);
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

    private Summary summary(String key, String firstLine, String action, String detail) {
      return summaries.computeIfAbsent(key, k -> new Summary(firstLine, action, detail));
    }

    /** Counts the outcome in the summary, with the records it stands for. */
    private void add(Summary summary, DecodeOutcome outcome) {
      summary.count++;
      if (outcome.recordCount() > 0) {
        summary.ranges.add(
            outcome.recordIndex(),
            outcome.recordIndex() + outcome.recordCount() - 1,
            outcome.span());
      }
      if (outcome.terminal()) {
        summary.ended = true;
        summary.endedAt = outcome.recordIndex();
      }
    }

    private void finish(long storedLength) {
      long size = decodedCount == 0 ? 0 : lastSpanEnd >= 0 ? lastSpanEnd : storedLength;
      if (size > 0) {
        context.dataSizeCounter().increase(size, getCallingUserTagAndEventTags(null, firstRecord));
      }
      for (Summary summary : summaries.values()) {
        StringBuilder text =
            new StringBuilder(summary.firstLine)
                .append("\nAction: ")
                .append(summary.action)
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
          if (summary.endedAfter >= 1) {
            text.append(" after recordIndex ").append(summary.endedAfter);
          } else if (summary.endedAt >= 1) {
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
              + "\nAction: fix this record where it is written, or the source's encodingType or"
              + " decoding if every record fails like this; the value is the record's bytes\n"
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
          log.debug("failed to write a DLQ entry after the stop", e);
          return;
        }
        List<String> lines = text.lines().toList();
        log.error(
            "failed to write a DLQ entry for s3://{}/{}, lost: {} / {}",
            ref.bucket(),
            ref.key(),
            lines.getFirst(),
            lines.getLast(),
            e);
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
