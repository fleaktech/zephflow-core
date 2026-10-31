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

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.filedecode.DlqTexts;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import io.fleak.zephflow.lib.commands.filedecode.UnitRun;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeResult;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.auto.AutoFormatCursor;
import io.fleak.zephflow.lib.serdes.decode.auto.FormatRecord;
import io.fleak.zephflow.lib.serdes.decode.config.AutoFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfigCompiler;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Decodes each message with the generic decoder. A message decodes all or nothing: one failed or
 * oversized outcome fails the whole message with a {@link MessageDecodeException}.
 */
@Slf4j
public class MessageDecodingConverter implements RawDataConverter<SerializedEvent> {

  /** What the source calls the bytes it decodes as one: a message, or a whole file. */
  public enum Unit {
    MESSAGE,
    FILE
  }

  private final RecordDecoder decoder;
  private final DecodeLimits limits;
  private final String format;
  private final Unit unit;
  private final DlqTexts texts;
  private final boolean automatic;

  /** Decodes without record or document limits, as the sources do. */
  public MessageDecodingConverter(DecodingConfig config, Unit unit) {
    this(config, unit, DecodeLimits.UNLIMITED);
  }

  public MessageDecodingConverter(DecodingConfig config, Unit unit, DecodeLimits limits) {
    this(
        DecodingConfigCompiler.compile(
            config,
            unit == Unit.MESSAGE ? AutoFormatCursor.Unit.MESSAGE : AutoFormatCursor.Unit.FILE),
        config.format(),
        unit,
        limits);
  }

  MessageDecodingConverter(RecordDecoder decoder, FormatConfig format, Unit unit) {
    this(decoder, format, unit, DecodeLimits.UNLIMITED);
  }

  private MessageDecodingConverter(
      RecordDecoder decoder, FormatConfig format, Unit unit, DecodeLimits limits) {
    this.decoder = decoder;
    this.limits = limits;
    this.format = SourceDecoding.formatName(format);
    this.unit = unit;
    this.automatic = format instanceof AutoFormatConfig;
    this.texts =
        new DlqTexts(
            this.format, SourceDecoding.multiline(format), 0, noun(unit), retry(unit), null, false);
  }

  /**
   * A message source's decoding config: {@code decoding} as given, or {@code encodingType}'s format
   * with gzip detection when {@code detectGzip}, and no decompression otherwise.
   *
   * @throws IllegalArgumentException when not exactly one of the two is set, or for an {@code
   *     encodingType} that cannot be decoded
   */
  public static DecodingConfig decodingConfig(
      EncodingType encodingType, DecodingConfig decoding, boolean detectGzip) {
    DecodingConfig config = new SourceDecoding(encodingType, null, decoding).decodingConfig();
    return decoding != null || detectGzip ? config : new DecodingConfig(List.of(), config.format());
  }

  @Override
  public ConvertedResult<SerializedEvent> convert(
      SerializedEvent sourceRecord, SourceExecutionContext<?> context) {
    return convert(sourceRecord, context, sourceRecord::toString);
  }

  /** As {@link #convert}, with {@code description} logged after the first line of a failure. */
  ConvertedResult<SerializedEvent> convert(
      SerializedEvent sourceRecord,
      SourceExecutionContext<?> context,
      Supplier<String> description) {
    byte[] value = sourceRecord.value();
    long size = value == null ? 0 : value.length;
    if (value == null) {
      return succeed(List.of(), size, sourceRecord, context);
    }
    List<RecordFleakData> records = null;
    MessageDecodeException failure;
    try {
      DecodeResult result = decoder.decodeAll(value, limits);
      if (result.problems().isEmpty()) {
        records = withMetadata(result.records(), sourceRecord);
        failure = null;
      } else {
        failure = problemFailure(result.problems());
      }
    } catch (Exception e) {
      failure =
          failure(
              "READ_FAILURE " + format + " " + noun(unit) + ": " + UnitRun.causeName(e),
              readFailureAction(),
              stackTrace(e),
              "",
              e);
    }
    if (failure == null) {
      return succeed(records, size, sourceRecord, context);
    }
    context.dataSizeCounter().increase(size, Map.of());
    context.deserializeFailureCounter().increase(Map.of());
    if (log.isErrorEnabled()) {
      log.error("failed to deserialize event: {}\n{}", logLine(failure), description.get());
    }
    return ConvertedResult.failure(failure, sourceRecord);
  }

  /** The first line, with the error's message: the first line names only its class. */
  private static String logLine(MessageDecodeException failure) {
    String message = failure.getCause() == null ? "" : UnitRun.causeMessage(failure.getCause());
    return message.isEmpty() ? failure.getMessage() : failure.getMessage() + ": " + message;
  }

  private ConvertedResult<SerializedEvent> succeed(
      List<RecordFleakData> records,
      long size,
      SerializedEvent sourceRecord,
      SourceExecutionContext<?> context) {
    Map<String, String> eventTags =
        getCallingUserTagAndEventTags(null, records.isEmpty() ? null : records.getFirst());
    context.dataSizeCounter().increase(size, eventTags);
    context.inputEventCounter().increase(records.size(), eventTags);
    if (log.isDebugEnabled()) {
      records.forEach(e -> log.debug("got message: {}", toJsonString(e)));
    }
    return ConvertedResult.success(records, sourceRecord);
  }

  /** Puts the message's metadata over each record's fields. */
  private List<RecordFleakData> withMetadata(
      List<RecordFleakData> records, SerializedEvent sourceRecord) {
    Map<String, String> metadata = SerializedEvent.metadataWithKey(sourceRecord);
    if (automatic) {
      Map<String, FleakData> values = new LinkedHashMap<>();
      metadata.forEach((key, value) -> values.put(key, new StringPrimitiveFleakData(value)));
      return records.stream().map(record -> FormatRecord.withMetadata(record, values)).toList();
    }
    for (RecordFleakData record : records) {
      metadata.forEach((k, v) -> record.getPayload().put(k, new StringPrimitiveFleakData(v)));
    }
    return records;
  }

  public boolean automatic() {
    return automatic;
  }

  private MessageDecodeException problemFailure(List<DecodeOutcome> problems) {
    DecodeOutcome first = problems.getFirst();
    String location = location(first, problems.size() - 1);
    MessageDecodeException result =
        switch (first) {
          case DecodeOutcome.Failed failed ->
              failure(
                  failed.kind()
                      + " "
                      + format
                      + " "
                      + noun(unit)
                      + ": "
                      + UnitRun.causeName(failed.error()),
                  failureAction(failed.kind()),
                  stackTrace(failed.error()),
                  location,
                  failed.error());
          case DecodeOutcome.Oversized oversized -> {
            DlqTexts.OversizedText text = texts.oversizedText(oversized);
            yield failure(
                "OVERSIZED " + format + " " + noun(unit) + ": " + text.label(),
                text.action(),
                text.detail(),
                location,
                null);
          }
          case DecodeOutcome.Decoded decoded ->
              throw new IllegalStateException("a decoded record is not a problem");
        };
    return new MessageDecodeException(result.getDlqText(), result.getCause(), problems);
  }

  private static MessageDecodeException failure(
      String firstLine, String action, String detail, String location, Throwable cause) {
    String text = firstLine + "\nAction: " + action + "\n" + detail;
    if (!location.isEmpty()) {
      text += "\n" + location;
    }
    return new MessageDecodeException(text, cause);
  }

  /** "recordIndex 2, bytes 10-25 (and 1 more problem)", from what is known. */
  private static String location(DecodeOutcome outcome, int others) {
    StringBuilder text = new StringBuilder();
    if (outcome.recordIndex() >= 1) {
      text.append("recordIndex ").append(outcome.recordIndex());
    }
    if (outcome.span() != null && outcome.span().length() >= 0) {
      text.append(text.isEmpty() ? "" : ", ")
          .append("bytes ")
          .append(outcome.span().offset())
          .append('-')
          .append(outcome.span().offset() + outcome.span().length());
    }
    if (others > 0) {
      String more = "and " + others + (others == 1 ? " more problem" : " more problems");
      text.append(text.isEmpty() ? more : " (" + more + ")");
    }
    return text.toString();
  }

  private String failureAction(ErrorKind kind) {
    return switch (kind) {
      case MALFORMED ->
          switch (unit) {
            case MESSAGE ->
                "check that the message is "
                    + format
                    + " as the source's encodingType or decoding says, and that its compression is"
                    + " set when the message is compressed, or fix the producer that wrote it; "
                    + retry(unit);
            case FILE ->
                "check that the file is "
                    + format
                    + " as the source's encodingType or decoding says, and that its compression is"
                    + " set when the file is compressed; "
                    + retry(unit);
          };
      case CORRUPT_COMPRESSION ->
          switch (unit) {
            case MESSAGE ->
                "check the source's compression setting against the message; otherwise the"
                    + " producer wrote damaged compressed data, or bytes after its end; "
                    + retry(unit);
            case FILE ->
                "check the source's compression setting against the file; otherwise the file holds"
                    + " damaged compressed data, or bytes after its end; "
                    + retry(unit);
          };
    };
  }

  private String readFailureAction() {
    return switch (unit) {
      case MESSAGE ->
          "the source could not process the message (the error says why); if every message fails"
              + " like this, check the source's encodingType or decoding; "
              + retry(unit);
      case FILE ->
          "the source could not process the file (the error says why); check the source's"
              + " encodingType or decoding, "
              + retry(unit);
    };
  }

  private static String stackTrace(Throwable error) {
    return ExceptionUtils.getStackTrace(error).stripTrailing();
  }

  private static String noun(Unit unit) {
    return switch (unit) {
      case MESSAGE -> "message";
      case FILE -> "file";
    };
  }

  private static String retry(Unit unit) {
    return switch (unit) {
      case MESSAGE -> "the DLQ value is the message as received, so it can be sent again";
      case FILE -> "fix the file and run the job again";
    };
  }
}
