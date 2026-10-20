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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.filedecode.DlqTexts.OversizedText;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import org.junit.jupiter.api.Test;

/** The texts of a source whose limits come from a setting; the heap texts are S3 realtime's. */
class DlqTextsTest {

  private static final String RENAME = "then upload the file under a new name";

  private static DlqTexts texts(String format) {
    return new DlqTexts(format, false, 2L << 30, "file", RENAME, "maxFileBytes", true);
  }

  @Test
  void theDocumentLimitNamesTheSetting() {
    assertEquals(
        new OversizedText(
            "maxDocumentBytes",
            "a document or block reached 300 bytes, over maxDocumentBytes 256",
            "this source sets maxDocumentBytes to maxFileBytes, so raise maxFileBytes (and the"
                + " task's heap to match), or write smaller documents or avro blocks; if one"
                + " document holds many records, decode it in a mode that reads them one at a"
                + " time (text line, json lines or array, xml elements, yaml multi_document,"
                + " protobuf varint_delimited, avro ocf; as encodingType: STRING_LINE,"
                + " JSON_OBJECT_LINE or JSON_ARRAY); "
                + RENAME),
        texts("json").oversizedText(oversized(LimitKind.MAX_DOCUMENT_BYTES)));
  }

  @Test
  void aParquetDocumentLimitAsksForSmallerRowGroups() {
    assertEquals(
        new OversizedText(
            "maxDocumentBytes",
            "a document or block reached 300 bytes, over maxDocumentBytes 256",
            "write the file with smaller row groups (fewer columns if the footer is too large),"
                + " or raise maxFileBytes (and the task's heap to match); "
                + RENAME),
        texts("parquet").oversizedText(oversized(LimitKind.MAX_DOCUMENT_BYTES)));
  }

  @Test
  void aMultilineEventLimitNamesTheSetting() {
    DlqTexts multiline = new DlqTexts("text", true, 0, "file", RENAME, "maxFileBytes", true);

    assertEquals(
        new OversizedText(
            "maxRecordBytes",
            "a multiline event reached 300 bytes, over maxRecordBytes 256",
            "check the decoding's multiline startPattern or continuePattern (a pattern that does"
                + " not match joins events until a limit); if events really are this large, this"
                + " source sets maxRecordBytes to maxFileBytes, so raise maxFileBytes (and the"
                + " task's heap to match); "
                + RENAME),
        multiline.oversizedText(oversized(LimitKind.MAX_RECORD_BYTES)));
  }

  @Test
  void theUnitIsCalledByTheSourcesNoun() {
    assertEquals(
        new OversizedText(
            "zstd window",
            "zstd data needs a window over 256 bytes",
            "compress the zstd data (the file, or an avro file's zstandard blocks) without long"
                + " mode, or with a window of 128 MiB or less (--long=27); no setting raises this"
                + " limit; "
                + RENAME),
        texts("json").oversizedText(oversized(LimitKind.ZSTD_WINDOW)));
    assertEquals(
        "check that the file is csv as the source's encodingType or decoding says, or fix the"
            + " listed records where the file is written; "
            + RENAME,
        texts("csv").failureAction(ErrorKind.MALFORMED));
  }

  @Test
  void aSummarizedCompressionFailureCanOnlyBeAnAvroBlock() {
    assertEquals(
        "a block's compressed data is damaged; rewrite the avro file; " + RENAME,
        texts("avro").failureAction(ErrorKind.CORRUPT_COMPRESSION));
  }

  private static DecodeOutcome.Oversized oversized(LimitKind limit) {
    return new DecodeOutcome.Oversized(1, 1, null, limit, 256, 300, new byte[0], false);
  }
}
