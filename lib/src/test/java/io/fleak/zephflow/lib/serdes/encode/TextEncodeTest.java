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
package io.fleak.zephflow.lib.serdes.encode;

import static io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec.GZIP;
import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

class TextEncodeTest {

  private static final String LINE = "{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}";
  private static final String MULTILINE =
      "{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"startPattern\":\"^<Event\"}}";
  private static final String ASA_1 =
      "%ASA-6-302013: Built inbound TCP connection 1 for outside:10.0.0.1/443";
  private static final String ASA_2 =
      "%ASA-6-302014: Teardown TCP connection 1 for outside:10.0.0.1/443 duration 0:00:01";
  private static final String ASA_3 = "%ASA-4-106023: Deny tcp src outside:10.0.0.9/5000";

  static final WriterCapability FILES =
      new WriterCapability(
          "filesink", Unit.MANY_RECORDS, Set.of("text"), Set.of(), false, Set.of(GZIP));
  static final WriterCapability RECORDS =
      new WriterCapability(
          "recordsink", Unit.ONE_RECORD, Set.of("text"), Set.of(), false, Set.of());

  @TempDir Path dir;

  @Test
  void recordModeWritesTheRawTextExactly() throws Exception {
    RecordEncoder encoder = EncodingCompiler.recordEncoder(format(LINE), RECORDS);
    assertEquals(
        List.of(ASA_1, "  padded line  ", "<Event>\r\n  <System/>\r\n</Event>\r\n"),
        List.of(
            utf8(encoder.encode(raw(ASA_1))),
            utf8(encoder.encode(raw("  padded line  "))),
            utf8(encoder.encode(raw("<Event>\r\n  <System/>\r\n</Event>\r\n")))));
  }

  @Test
  void fileModeEndsEveryRecordWithALineFeed() throws Exception {
    BlobFileWriter<RecordFleakData> lines = EncodingCompiler.fileWriter(format(LINE), FILES);
    BlobFileWriter<RecordFleakData> multiline =
        EncodingCompiler.fileWriter(format(MULTILINE), FILES);
    assertEquals(
        List.of(ASA_1 + "\n" + ASA_2 + "\n" + ASA_3 + "\n", "a\nb\r\nc\n", "", "txt"),
        List.of(
            utf8(writeOneFile(lines, List.of(raw(ASA_1), raw(ASA_2), raw(ASA_3)), dir)),
            utf8(writeOneFile(multiline, List.of(raw("a"), raw("b\r\nc")), dir)),
            utf8(writeOneFile(lines, List.of(), dir)),
            lines.getFileExtension()));
  }

  @Test
  void gzippedFilesHoldThePlainBytes() throws Exception {
    BlobFileWriter<RecordFleakData> writer =
        EncodingCompiler.fileWriter(encoding("[\"gzip\"]", LINE), FILES);
    byte[] gzipped = writeOneFile(writer, List.of(raw(ASA_1), raw(ASA_2)), dir);
    try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(gzipped))) {
      assertEquals(
          List.of(ASA_1 + "\n" + ASA_2 + "\n", "txt.gz"),
          List.of(utf8(in.readAllBytes()), writer.getFileExtension()));
    }
  }

  @Test
  void recordsWithoutStringRawTextAreRefused() {
    RecordEncoder encoder = EncodingCompiler.recordEncoder(format(LINE), RECORDS);
    BlobFileWriter<RecordFleakData> files = EncodingCompiler.fileWriter(format(LINE), FILES);
    String missing = "record has no string __raw__ field to write as text";
    assertEquals(
        List.of(missing, missing, missing, missing),
        List.of(
            failure(() -> encoder.encode(json("{\"message\":\"x\"}"))),
            failure(() -> encoder.encode(json("{\"__raw__\":42}"))),
            failure(() -> files.validateRecord(json("{\"__raw__\":null}"))),
            failure(() -> files.writeToTempFiles(List.of(raw(ASA_1), json("{}")), dir))));
  }

  @Test
  void textRoundTripsThroughTheSameFormatConfig() throws Exception {
    String xml = "<Event a=\"1\">\r\n  <System/>\r\n</Event>";
    List<RecordFleakData> lineRecords = List.of(raw(ASA_1), raw(ASA_2), raw(ASA_3));
    List<RecordFleakData> multilineRecords = List.of(raw(xml), raw(xml.replace("1", "2")));
    assertEquals(
        List.of(lineRecords, multilineRecords),
        List.of(roundTrip(LINE, lineRecords), roundTrip(MULTILINE, multilineRecords)));
  }

  @Test
  void rawTextBytesAreUtf8() throws Exception {
    RecordEncoder encoder = EncodingCompiler.recordEncoder(format(LINE), RECORDS);
    assertArrayEquals(
        "Zürich 東京".getBytes(StandardCharsets.UTF_8), encoder.encode(raw("Zürich 東京")));
  }

  private List<RecordFleakData> roundTrip(String format, List<RecordFleakData> records)
      throws Exception {
    byte[] file = writeOneFile(EncodingCompiler.fileWriter(format(format), FILES), records, dir);
    DecodingConfig decoding = decoding("[]", format);
    return decode(decoding, file).records();
  }

  private static RecordFleakData raw(String text) {
    return (RecordFleakData) FleakData.wrap(Map.of("__raw__", text));
  }

  private static String failure(Executable action) {
    return assertThrows(IllegalArgumentException.class, action).getMessage();
  }

  private static String utf8(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
