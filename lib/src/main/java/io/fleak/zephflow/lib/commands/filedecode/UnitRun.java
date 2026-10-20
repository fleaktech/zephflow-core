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
package io.fleak.zephflow.lib.commands.filedecode;

import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.NodeExecutionException;
import io.fleak.zephflow.lib.commands.filedecode.DlqTexts.OversizedText;
import io.fleak.zephflow.lib.commands.source.BatchEmittingConverter.Emitter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

/**
 * Handles the decoder outcomes of one unit (an object or a file): emits its records in bounded
 * batches while it is decoded, dead-letters a bad record that carries at most 128 KiB with its own
 * bytes, and summarizes every other failure once per unit and kind. Depending on the {@link
 * Policy}, a read or downstream failure is summarized too, or ends the unit early without its
 * summaries so that it can be read again.
 */
@Slf4j
public final class UnitRun {

  public static final int MAX_RECORD_ENTRY_BYTES = 128 * 1024;
  public static final int MAX_BATCH_RECORDS = 1000;
  private static final long VALUE_BYTES = 8;
  private static final String UNKNOWN_NODE = "unknown";

  /** What every unit of a source shares. */
  public record Settings(
      DlqTexts texts, Policy policy, long batchBytes, DlqWriter dlqWriter, String nodeId) {
    public Settings {
      if (texts.blockCodecOnly() != policy.endEarly()) {
        throw new IllegalArgumentException(
            "the texts' blockCodecOnly must match the policy's endEarly");
      }
    }
  }

  /**
   * How a source handles failures.
   *
   * @param endEarly whether a read failure, a downstream failure and a terminal {@code
   *     CORRUPT_COMPRESSION} end the unit early, without its summaries, instead of being summarized
   * @param keepSmallDocuments whether a terminal {@code Failed} of a whole document (record index
   *     -1) is dead-lettered with its bytes when they fit, like a bad record
   * @param summarizeWithoutDlq whether, without a DLQ, bad records are counted into summaries and
   *     each summary is logged once at error, instead of each entry being logged at warn
   */
  public record Policy(boolean endEarly, boolean keepSmallDocuments, boolean summarizeWithoutDlq) {
    public static final Policy SUMMARIZE = new Policy(false, false, false);
    public static final Policy END_EARLY = new Policy(true, true, true);
  }

  public record Counters(
      FleakCounter inputEvents, FleakCounter inputBytes, FleakCounter decodeFailures) {}

  /** The unit being decoded: its name in texts and the values of its DLQ entries. */
  public interface Unit {

    /** How texts name the unit, such as "object s3://b/k version v1" or "file s3://b/k". */
    String name();

    /** Where the unit is, for logs: "s3://b/k". */
    String location();

    /** The value of a bad record's entry. */
    SerializedEvent recordValue(DecodeOutcome.Failed failed);

    /** The value of a summary's entry. */
    SerializedEvent summaryValue();
  }

  /** How a unit ended. */
  public sealed interface End {}

  /** Every outcome was handled and the summaries were written. */
  public record Finished() implements End {}

  /** The source was told to stop; nothing more was written. */
  public record Stopped() implements End {}

  /**
   * Reading the unit failed; nothing more was emitted or written (an {@code endEarly} policy). The
   * input bytes counted are those through the last record emitted.
   */
  public record ReadFailed(Exception error) implements End {}

  /** The downstream failed a batch; nothing more was emitted or written ({@code endEarly}). */
  public record DownstreamFailed(Exception error) implements End {}

  /** The failures of one kind in one unit, summarized into one DLQ entry. */
  private static final class Summary {
    final String firstLine;
    final String action;
    final String firstDetail;
    final RecordRanges ranges = new RecordRanges();
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

  /** The downstream failures at one node for one unit. */
  private static final class Downstream {
    final String stackTrace;
    final RecordRanges ranges = new RecordRanges();
    long batches;

    Downstream(String stackTrace) {
      this.stackTrace = stackTrace;
    }
  }

  private final Settings settings;
  private final DlqTexts texts;
  private final Counters counters;
  private final Unit unit;
  private final Emitter emitter;
  private final Map<String, Summary> summaries = new LinkedHashMap<>();
  private final Map<String, Downstream> downstream = new LinkedHashMap<>();
  private List<RecordFleakData> batch = new ArrayList<>();
  private long batchSize;
  private long batchFirstIndex = -1;
  private long batchLastIndex = -1;
  private long lastSpanEnd = -1;
  private long batchSpanEnd = -1;
  private long emittedSpanEnd = -1;
  private long decodedCount;
  private long lastRecordIndex;
  private RecordFleakData firstRecord;
  private End endedEarly;

  public UnitRun(Settings settings, Counters counters, Unit unit, Emitter emitter) {
    this.settings = settings;
    this.texts = settings.texts();
    this.counters = counters;
    this.unit = unit;
    this.emitter = emitter;
  }

  /** The record-size estimate batches are capped by: 8 bytes per value and field, plus text. */
  public static long estimate(FleakData data) {
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
  public static String causeName(Throwable error) {
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

  /** Handles one outcome; returns false when no more outcomes are wanted. */
  public boolean visit(DecodeOutcome outcome) {
    if (endedEarly != null || emitter.stopped()) {
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
        if (decoded.span() != null && decoded.span().length() >= 0) {
          batchSpanEnd = Math.max(batchSpanEnd, decoded.span().offset() + decoded.span().length());
        }
        batch.add(decoded.record());
        batchSize += estimate(decoded.record());
        if (firstRecord == null) {
          firstRecord = decoded.record();
        }
        decodedCount++;
        if (batch.size() >= MAX_BATCH_RECORDS || batchSize >= settings.batchBytes()) {
          flush();
        }
      }
      case DecodeOutcome.Failed failed -> {
        track(failed.span());
        if (settings.policy().endEarly()
            && failed.terminal()
            && failed.kind() == ErrorKind.CORRUPT_COMPRESSION) {
          endedEarly = new ReadFailed(failed.error());
          return false;
        }
        counters.decodeFailures().increase(Map.of());
        if (isRecordEntry(failed)) {
          writeRecordEntry(failed);
        } else {
          add(
              summary(
                  failed.kind().name(),
                  failed.kind()
                      + " "
                      + texts.format()
                      + " "
                      + texts.noun()
                      + ": "
                      + causeName(failed.error()),
                  texts.failureAction(failed.kind()),
                  ExceptionUtils.getStackTrace(failed.error())),
              failed);
        }
      }
      case DecodeOutcome.Oversized oversized -> {
        track(oversized.span());
        counters.decodeFailures().increase(Map.of());
        OversizedText text = texts.oversizedText(oversized);
        add(
            summary(
                "OVERSIZED " + oversized.limit(),
                "OVERSIZED " + texts.format() + " " + texts.noun() + ": " + text.label(),
                text.action(),
                text.detail()),
            oversized);
      }
    }
    return endedEarly == null && !emitter.stopped();
  }

  /**
   * Ends the unit early on a read failure, without emitting the open batch or writing summaries
   * ({@code endEarly} policy).
   */
  public void readFailed(Exception error) {
    if (endedEarly == null) {
      endedEarly = new ReadFailed(error);
    }
  }

  /**
   * Emits the open batch, summarizes a failure that ended the unit, after the last record seen, and
   * writes the summaries (a policy that summarizes read failures).
   */
  public void finishWithFailure(String key, String firstLine, String action, Exception error) {
    if (emitter.stopped()) {
      return;
    }
    flush();
    if (emitter.stopped()) {
      return;
    }
    counters.decodeFailures().increase(Map.of());
    Summary summary = summary(key, firstLine, action, ExceptionUtils.getStackTrace(error));
    summary.count++;
    summary.ended = true;
    summary.endedAfter = lastRecordIndex;
    finish(-1);
  }

  /**
   * Emits the open batch and writes the summaries, unless the unit ended early or the source was
   * told to stop.
   *
   * @param storedLength the unit's size, counted as input bytes when no outcome had a span
   */
  public End complete(long storedLength) {
    if (endedEarly != null) {
      return endEarly();
    }
    if (emitter.stopped()) {
      return new Stopped();
    }
    flush();
    if (endedEarly != null) {
      return endEarly();
    }
    if (emitter.stopped()) {
      return new Stopped();
    }
    finish(storedLength);
    return new Finished();
  }

  private End endEarly() {
    if (emittedSpanEnd > 0) {
      counters
          .inputBytes()
          .increase(emittedSpanEnd, getCallingUserTagAndEventTags(null, firstRecord));
    }
    return endedEarly;
  }

  /** Writes an entry with the summary value, such as a unit too large to read, under the source. */
  public void writeEntry(String text) {
    write(unit.summaryValue(), text, settings.nodeId());
  }

  private boolean isRecordEntry(DecodeOutcome.Failed failed) {
    if (settings.dlqWriter() == null && settings.policy().summarizeWithoutDlq()) {
      return false;
    }
    boolean fits = failed.raw().length > 0 && failed.raw().length <= MAX_RECORD_ENTRY_BYTES;
    boolean wholeDocument = settings.policy().keepSmallDocuments() && failed.recordIndex() == -1;
    return fits && (!failed.terminal() || wholeDocument);
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
    emittedSpanEnd = Math.max(emittedSpanEnd, batchSpanEnd);
    counters
        .inputEvents()
        .increase(emitting.size(), getCallingUserTagAndEventTags(null, emitting.getFirst()));
    try {
      emitter.accept(emitting);
    } catch (Exception e) {
      if (settings.policy().endEarly()) {
        endedEarly = new DownstreamFailed(e);
        return;
      }
      // After a stop the summaries are never written, so this one is dropped with them.
      String failingNode = e instanceof NodeExecutionException nee ? nee.getNodeId() : UNKNOWN_NODE;
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
          outcome.recordIndex(), outcome.recordIndex() + outcome.recordCount() - 1, outcome.span());
    }
    if (outcome.terminal()) {
      summary.ended = true;
      summary.endedAt = outcome.recordIndex();
    }
  }

  private void finish(long storedLength) {
    long size = decodedCount == 0 ? 0 : lastSpanEnd >= 0 ? lastSpanEnd : storedLength;
    if (size > 0) {
      counters.inputBytes().increase(size, getCallingUserTagAndEventTags(null, firstRecord));
    }
    for (Summary summary : summaries.values()) {
      StringBuilder text =
          new StringBuilder(summary.firstLine)
              .append("\nAction: ")
              .append(summary.action)
              .append('\n')
              .append(summary.firstDetail)
              .append('\n')
              .append(unit.name())
              .append("; ")
              .append(summary.count)
              .append(summary.count == 1 ? " outcome" : " outcomes");
      if (!summary.ranges.isEmpty()) {
        text.append("; ").append(summary.ranges.describe());
      }
      if (summary.ended) {
        text.append("; the ").append(texts.noun()).append(" ended");
        if (summary.endedAfter >= 1) {
          text.append(" after recordIndex ").append(summary.endedAfter);
        } else if (summary.endedAt >= 1) {
          text.append(" at recordIndex ").append(summary.endedAt);
        }
      }
      writeSummary(text.toString());
    }
    for (Map.Entry<String, Downstream> entry : downstream.entrySet()) {
      Downstream failure = entry.getValue();
      String text =
          failure.stackTrace
              + "\n"
              + unit.name()
              + "; "
              + failure.batches
              + (failure.batches == 1 ? " batch" : " batches")
              + " failed; "
              + failure.ranges.describe();
      write(unit.summaryValue(), text, entry.getKey());
    }
  }

  private void writeSummary(String text) {
    if (settings.dlqWriter() == null && settings.policy().summarizeWithoutDlq()) {
      log.error("dropping failures of {} (no DLQ configured): {}", unit.location(), text);
      return;
    }
    writeEntry(text);
  }

  private void writeRecordEntry(DecodeOutcome.Failed failed) {
    StringBuilder identity = new StringBuilder(unit.name());
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
            + texts.format()
            + " record: "
            + causeName(failed.error())
            + "\nAction: fix this record where it is written, or the source's encodingType or"
            + " decoding if every record fails like this; the value is the record's bytes\n"
            + ExceptionUtils.getStackTrace(failed.error())
            + "\n"
            + identity;
    write(unit.recordValue(failed), text, settings.nodeId());
  }

  private void write(SerializedEvent value, String text, String entryNodeId) {
    if (settings.dlqWriter() == null) {
      log.warn(
          "dropping a failure of {} (no DLQ configured): {}",
          unit.location(),
          text.lines().findFirst().orElse(""));
      return;
    }
    try {
      settings.dlqWriter().writeToDlq(System.currentTimeMillis(), value, text, entryNodeId);
    } catch (RuntimeException e) {
      if (emitter.stopped()) {
        log.debug("failed to write a DLQ entry after the stop", e);
        return;
      }
      List<String> lines = text.lines().toList();
      log.error(
          "failed to write a DLQ entry for {}, lost: {} / {}",
          unit.location(),
          lines.getFirst(),
          lines.getLast(),
          e);
    }
  }
}
