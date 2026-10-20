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

import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.compress.Decompressors;

/**
 * What a source's DLQ texts say about the outcomes of one format, and what the operator can do.
 *
 * @param noun what the source calls a unit: {@code object} or {@code file}
 * @param retry how to have the unit read again, the end of every action
 * @param heapBytes the heap the limits are proportions of; 0 when they are not shares of a known
 *     heap
 * @param limitSetting the setting the record and document limits come from, or null when they come
 *     from the heap
 * @param blockCodecOnly whether a {@code CORRUPT_COMPRESSION} that reaches a summary can only be an
 *     avro block's own codec (the unit's compression ends the unit as a read failure)
 */
public record DlqTexts(
    String format,
    boolean multiline,
    long heapBytes,
    String noun,
    String retry,
    String limitSetting,
    boolean blockCodecOnly) {

  /** The words after {@code "OVERSIZED <format> <noun>: "}, the detail line, and what to do. */
  public record OversizedText(String label, String detail, String action) {}

  /**
   * What an oversized outcome went over and what the operator can do. The record, document and
   * codec limits come from the task's heap, or from {@link #limitSetting}, so more memory or that
   * setting raises them; zstd's window and the format maximum cannot be raised.
   */
  public OversizedText oversizedText(DecodeOutcome.Oversized oversized) {
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
                      + raise("maxRecordBytes", value, "event")
                      + "; "
                      + retry
                  : raise("maxRecordBytes", value, "record")
                      + ", or write smaller records; if records should be much smaller, check"
                      + " that the source's format matches the "
                      + noun
                      + " (a wrong format or framing, or an unclosed csv quote, joins many records"
                      + " into one); "
                      + retry);
      case MAX_DOCUMENT_BYTES ->
          new OversizedText(
              "maxDocumentBytes",
              "a document or block reached " + size + " bytes, over maxDocumentBytes " + value,
              limitSetting != null && format.equals("parquet")
                  ? "write the file with smaller row groups (fewer columns if the footer is too"
                      + " large), or raise "
                      + limitSetting
                      + " (and the task's heap to match); "
                      + retry
                  : raise("maxDocumentBytes", value, "document or block")
                      + ", or write smaller documents or avro blocks; if one document holds many"
                      + " records, decode it in a mode that reads them one at a time (text line,"
                      + " json lines or array, xml elements, yaml multi_document, protobuf"
                      + " varint_delimited, avro ocf; as encodingType: STRING_LINE,"
                      + " JSON_OBJECT_LINE or JSON_ARRAY); "
                      + retry);
      case MAX_CODEC_MEMORY_BYTES ->
          new OversizedText(
              "xz memory",
              "xz needs " + size + " bytes of memory, over its limit of " + value,
              xzMemory(value)
                  + "recompress the "
                  + noun
                  + " with a smaller xz dictionary (the presets -0 to -9 need at most 65 MiB)"
                  + xzFixed(value)
                  + "; "
                  + retry);
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
                  + retry);
      case HEAP_SHARE ->
          new OversizedText(
              "parquet heap share",
              "parquet needs " + size + " bytes, over its share of the heap, " + value,
              "give the task more memory, or write the file with smaller row groups or rows"
                  + " (fewer row groups or columns if the footer is too large); "
                  + retry);
      case MAX_LINES ->
          new OversizedText(
              "maxLines",
              "a multiline event has more than " + value + " lines (" + size + " bytes)",
              "raise maxLines in the source's decoding, or fix its multiline pattern if events"
                  + " run together; "
                  + retry);
      case ZSTD_WINDOW ->
          new OversizedText(
              "zstd window",
              "zstd data needs a window over " + value + " bytes",
              "compress the zstd data (the "
                  + noun
                  + ", or an avro file's zstandard blocks) without long mode, or with a window of"
                  + " 128 MiB or less (--long=27); no setting raises this limit; "
                  + retry);
      case NATIVE_MEMORY ->
          new OversizedText(
              "zstd native memory",
              "zstd could not allocate native memory",
              "the worker ran out of memory outside the Java heap: give the worker more memory"
                  + " or run fewer tasks on it; if it was momentary, "
                  + retry);
      case FORMAT_MAXIMUM ->
          new OversizedText(
              "format maximum",
              unit()
                  + " reached "
                  + size
                  + " bytes, over the "
                  + value
                  + " bytes the decoder can hold in one piece",
              "write the file in smaller pieces (avro: smaller blocks; protobuf: smaller"
                  + " messages; parquet: a smaller footer); no setting raises this limit; "
                  + retry);
    };
  }

  /** What to do about a failure that is not over a limit. */
  public String failureAction(ErrorKind kind) {
    return switch (kind) {
      case MALFORMED ->
          "check that the "
              + noun
              + " is "
              + format
              + " as the source's encodingType or decoding says, or fix the listed records where"
              + " the "
              + noun
              + " is written; "
              + retry;
      case CORRUPT_COMPRESSION ->
          blockCodecOnly
              ? "a block's compressed data is damaged; rewrite the avro file; " + retry
              : "check the source's compression setting against the "
                  + noun
                  + ", and that the "
                  + noun
                  + " was complete when it was announced; otherwise its compressed data (for"
                  + " avro, possibly a block's own codec data) is damaged or has bytes after its"
                  + " end, so rewrite the "
                  + noun
                  + "; "
                  + retry;
    };
  }

  /**
   * "this source sets maxRecordBytes to 1/128 of the task's heap, so give the task a heap of at
   * least ...", or how to raise {@link #limitSetting}.
   */
  private String raise(String setting, long value, String unit) {
    if (limitSetting != null) {
      return "this source sets "
          + setting
          + " to "
          + limitSetting
          + ", so raise "
          + limitSetting
          + " (and the task's heap to match)";
    }
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

  private String unit() {
    return switch (format) {
      case "avro" -> "an avro block";
      case "protobuf" -> "a protobuf message";
      case "parquet" -> "the parquet footer";
      default -> "a unit";
    };
  }
}
