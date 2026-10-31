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
package io.fleak.zephflow.lib.serdes.decode;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.decode.auto.AutoFormatCursor;
import io.fleak.zephflow.lib.serdes.decode.auto.DetectedFormat;
import io.fleak.zephflow.lib.serdes.decode.auto.FormatDetector;
import io.fleak.zephflow.lib.serdes.decode.auto.FormatRecord;
import io.fleak.zephflow.lib.serdes.decode.auto.UnsupportedFormatException;
import io.fleak.zephflow.lib.serdes.decode.config.AutoFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfigCompiler;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class AutoFormatTest {
  private static final DecodeLimits LIMITS = new DecodeLimits(16L * 1024 * 1024, 32L * 1024 * 1024);

  private RecordDecoder decoder(AutoFormatCursor.Unit unit) {
    return DecodingConfigCompiler.compile(
        new DecodingConfig(List.of(DecompressionCodec.AUTO), new AutoFormatConfig()), unit);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      textBlock =
          """
      {"a":1}|json_object
      [{"a":1},false,null]|json_array
      []|json_array
      a: b|yaml
      a: b,c|yaml
      a,b|csv
      hello|unrecognized_text
      null|unrecognized_text
      123|unrecognized_text
      "abc"|unrecognized_text
      {"a":|unrecognized_text
      <events/>|unrecognized_text
      """)
  void recognizesWholeMessageAndProjectsOneRecord(String input, String expected) throws Exception {
    var result =
        decoder(AutoFormatCursor.Unit.MESSAGE)
            .decodeAll(input.getBytes(StandardCharsets.UTF_8), LIMITS);
    assertTrue(result.problems().isEmpty(), result.problems().toString());
    assertEquals(1, result.records().size());
    var envelope = result.records().getFirst();
    assertEquals(expected, FormatRecord.format(envelope).id());
    var projected = FormatRecord.project(envelope);
    assertEquals(
        expected.equals("json_object") ? Map.of("a", 1L) : Map.of("__raw__", input),
        projected.unwrap());
  }

  @Test
  void validatesSyslogAndCefInsteadOfAcceptingPrefixes() {
    String cef = "CEF:0|vendor|product|1|event|name|5|src=10.0.0.1 msg=hello world";
    String modern = "<34>1 2003-10-11T22:14:15.003Z host app 123 ID47 [example@1 x=\"a\\]b\"] ";
    String legacy = "<34>Oct 11 22:14:15 host app[123]: ";
    assertEquals(DetectedFormat.SYSLOG_CEF, FormatRecord.format(FormatDetector.classify(cef)));
    assertEquals(
        DetectedFormat.SYSLOG_CEF, FormatRecord.format(FormatDetector.classify(modern + cef)));
    assertEquals(
        DetectedFormat.SYSLOG_CEF, FormatRecord.format(FormatDetector.classify(legacy + cef)));
    assertEquals(
        DetectedFormat.SYSLOG_PLAIN,
        FormatRecord.format(FormatDetector.classify(modern + "CEF:bad")));
    assertNotEquals(
        DetectedFormat.SYSLOG_PLAIN,
        FormatRecord.format(FormatDetector.classify("<999>1 invalid header")));
    assertNotEquals(
        DetectedFormat.SYSLOG_CEF,
        FormatRecord.format(
            FormatDetector.classify("CEF:whatever|vendor|product|1|event|name|5|")));
  }

  @Test
  void preservesMetadataWithoutInterpretingPayloadFieldNames() {
    var payload =
        (RecordFleakData)
            FleakData.wrap(
                Map.of(
                    "formatId",
                    "csv",
                    "value",
                    5,
                    "__raw__",
                    "a,b",
                    "__format__",
                    Map.of("id", "csv")));
    var envelope =
        FormatRecord.withMetadata(
            FormatRecord.structured(payload), Map.of("key", FleakData.wrap("k")));
    var projected = FormatRecord.project(envelope);
    assertEquals(DetectedFormat.JSON_OBJECT, FormatRecord.format(envelope));
    assertEquals(payload.getPayload().get("__format__"), projected.getPayload().get("__format__"));
    assertEquals("k", projected.getPayload().get("key").unwrap());
    assertThrows(IllegalArgumentException.class, () -> FormatRecord.project(payload));
  }

  @Test
  void messageKeepsMultilineCsvWhileFileKeepsEachLogicalRowAndHeader() throws Exception {
    String text = "a,b\n\"hello\nworld\",2\n";
    var message =
        decoder(AutoFormatCursor.Unit.MESSAGE)
            .decodeAll(text.getBytes(StandardCharsets.UTF_8), LIMITS);
    assertEquals(1, message.records().size());
    assertEquals(text, FormatRecord.text(message.records().getFirst()));
    var file =
        decoder(AutoFormatCursor.Unit.FILE)
            .decodeAll(text.getBytes(StandardCharsets.UTF_8), LIMITS);
    assertTrue(file.problems().isEmpty());
    assertEquals(
        List.of("a,b", "\"hello\nworld\",2"),
        file.records().stream().map(FormatRecord::text).toList());
  }

  @Test
  void emptyMessageAndEmptyFileHaveDifferentBoundaries() throws Exception {
    assertEquals(
        1, decoder(AutoFormatCursor.Unit.MESSAGE).decodeAll(new byte[0], LIMITS).records().size());
    assertEquals(
        0, decoder(AutoFormatCursor.Unit.FILE).decodeAll(new byte[0], LIMITS).records().size());
  }

  @Test
  void wholeArrayPreservesOriginalWhitespaceAndIsNotFlattened() throws Exception {
    String text = " \n[\n {\"a\": 1},\n false, null\n]\n ";
    var result =
        decoder(AutoFormatCursor.Unit.FILE)
            .decodeAll(text.getBytes(StandardCharsets.UTF_8), LIMITS);
    assertTrue(result.problems().isEmpty());
    assertEquals(1, result.records().size());
    assertEquals(
        text,
        FormatRecord.project(result.records().getFirst()).getPayload().get("__raw__").unwrap());
  }

  @Test
  void keepsJsonDocumentsAndReadableTruncatedTail() throws Exception {
    String text = "{\"a\":1}\n[1,2]\n{\"b\":";
    var result =
        decoder(AutoFormatCursor.Unit.FILE)
            .decodeAll(text.getBytes(StandardCharsets.UTF_8), LIMITS);
    assertTrue(result.problems().isEmpty());
    assertEquals(
        List.of(
            DetectedFormat.JSON_OBJECT,
            DetectedFormat.JSON_ARRAY,
            DetectedFormat.UNRECOGNIZED_TEXT),
        result.records().stream().map(FormatRecord::format).toList());
    assertEquals("[1,2]", FormatRecord.text(result.records().get(1)));
    assertEquals("{\"b\":", FormatRecord.text(result.records().get(2)));
  }

  @Test
  void unicodeYamlKeyKeepsTheWholeDocumentBeforeLineFraming() throws Exception {
    String text = "ą".repeat(600) + ": 1\nb: 2\n";
    for (var unit : AutoFormatCursor.Unit.values()) {
      var result = decoder(unit).decodeAll(text.getBytes(StandardCharsets.UTF_8), LIMITS);
      assertEquals(1, result.records().size(), unit.name());
      assertEquals(DetectedFormat.YAML, FormatRecord.format(result.records().getFirst()));
      assertEquals(text, FormatRecord.text(result.records().getFirst()));
    }
  }

  @Test
  void yamlDocumentsWinOverLineFraming() throws Exception {
    for (String text : List.of("a: b\nc: d\n", "{\"a\":1}\n# yaml comment\n")) {
      var result =
          decoder(AutoFormatCursor.Unit.FILE)
              .decodeAll(text.getBytes(StandardCharsets.UTF_8), LIMITS);
      assertEquals(1, result.records().size());
      assertEquals(DetectedFormat.YAML, FormatRecord.format(result.records().getFirst()));
      assertEquals(text, FormatRecord.text(result.records().getFirst()));
    }
    var documents =
        decoder(AutoFormatCursor.Unit.FILE)
            .decodeAll("---\na: żółć\n---\n- x\n- y\n".getBytes(StandardCharsets.UTF_8), LIMITS);
    assertEquals(2, documents.records().size());
    assertEquals(
        List.of("---\na: żółć\n", "---\n- x\n- y\n"),
        documents.records().stream().map(FormatRecord::text).toList());
  }

  @ParameterizedTest
  @ValueSource(strings = {"csv", "jsonl"})
  void largeFileOfSmallRecordsDoesNotReceiveDocumentLimitAndOpensInputOnce(String format)
      throws Exception {
    String row =
        format.equals("csv")
            ? "a," + "x".repeat(1021) + "\n"
            : "{\"v\":\"" + "x".repeat(1015) + "\"}\n";
    byte[] bytes = row.getBytes(StandardCharsets.UTF_8);
    assertEquals(1024, bytes.length);
    int count = 48 * 1024;
    var opens = new AtomicInteger();
    var closes = new AtomicInteger();
    ByteInput input =
        () -> {
          assertEquals(1, opens.incrementAndGet());
          return new InputStream() {
            long position;

            public int read() {
              return position == (long) bytes.length * count
                  ? -1
                  : bytes[(int) (position++ % bytes.length)] & 255;
            }

            public int read(byte[] target, int offset, int length) {
              if (position == (long) bytes.length * count) return -1;
              int copied = Math.min(length, bytes.length - (int) (position % bytes.length));
              System.arraycopy(bytes, (int) (position % bytes.length), target, offset, copied);
              position += copied;
              return copied;
            }

            public void close() {
              closes.incrementAndGet();
            }
          };
        };
    int records = 0;
    try (var cursor = decoder(AutoFormatCursor.Unit.FILE).open(input, LIMITS)) {
      DecodeOutcome outcome;
      while ((outcome = cursor.next()) != null) {
        assertInstanceOf(DecodeOutcome.Decoded.class, outcome);
        records++;
      }
    }
    assertEquals(count, records);
    assertEquals(1, opens.get());
    assertEquals(1, closes.get());
  }

  @Test
  void documentLimitDoesNotBecomeUnrecognizedText() throws Exception {
    String text = "{\"v\":\"" + "x".repeat(64) + "\"}";
    var result =
        decoder(AutoFormatCursor.Unit.FILE)
            .decodeAll(text.getBytes(StandardCharsets.UTF_8), new DecodeLimits(16, 32));
    assertTrue(result.records().isEmpty());
    assertInstanceOf(DecodeOutcome.Oversized.class, result.problems().getFirst());
  }

  @Test
  void distinguishesUnsupportedBinaryMalformedEncodingCompressionAndReadFailure() throws Exception {
    var decoder = decoder(AutoFormatCursor.Unit.MESSAGE);
    var binary = decoder.decodeAll(new byte[] {'P', 'A', 'R', '1', 0, 0, 0, 0}, LIMITS);
    assertInstanceOf(
        UnsupportedFormatException.class,
        ((DecodeOutcome.Failed) binary.problems().getFirst()).error());
    assertInstanceOf(
        DecodeOutcome.Failed.class,
        decoder.decodeAll(new byte[] {(byte) 0xC3, 0x28}, LIMITS).problems().getFirst());
    assertInstanceOf(
        DecodeOutcome.Failed.class,
        decoder
            .decodeAll(new byte[] {(byte) 0xFF, (byte) 0xFE, 0, (byte) 0xD8}, LIMITS)
            .problems()
            .getFirst());
    var compressed = new ByteArrayOutputStream();
    try (var gzip = new GZIPOutputStream(compressed)) {
      gzip.write("{\"a\":1}".getBytes(StandardCharsets.UTF_8));
    }
    byte[] damaged = compressed.toByteArray();
    damaged[damaged.length - 1] ^= 1;
    var failure = decoder.decodeAll(damaged, LIMITS);
    assertEquals(
        ErrorKind.CORRUPT_COMPRESSION,
        ((DecodeOutcome.Failed) failure.problems().getFirst()).kind());
    ByteInput interrupted =
        () ->
            new InputStream() {
              int count;

              public int read() throws IOException {
                if (count++ < 12) return 'a';
                throw new IOException("transport interrupted");
              }
            };
    try (var cursor = decoder(AutoFormatCursor.Unit.FILE).open(interrupted, LIMITS)) {
      assertThrows(SourceReadException.class, cursor::next);
    }
  }

  @Test
  void autoConfigurationRoundTripsWithoutReadingInput() throws Exception {
    var config =
        JsonUtils.OBJECT_MAPPER.readValue(
            "{\"compression\":\"auto\",\"format\":{\"type\":\"auto\"}}", DecodingConfig.class);
    assertInstanceOf(AutoFormatConfig.class, config.format());
    assertEquals(
        config,
        JsonUtils.OBJECT_MAPPER.readValue(JsonUtils.toJsonString(config), DecodingConfig.class));
    assertNotNull(DecodingConfigCompiler.compile(config));
  }

  @Test
  void mixedLogLinesRemainIndependentAfterJson() throws Exception {
    var result =
        decoder(AutoFormatCursor.Unit.FILE)
            .decodeAll("{\"a\":1}\nhello\n{\"b\":2}\n".getBytes(StandardCharsets.UTF_8), LIMITS);
    assertTrue(result.problems().isEmpty());
    assertEquals(
        List.of(
            DetectedFormat.JSON_OBJECT,
            DetectedFormat.UNRECOGNIZED_TEXT,
            DetectedFormat.JSON_OBJECT),
        result.records().stream().map(FormatRecord::format).toList());
    assertEquals("hello", FormatRecord.text(result.records().get(1)));
  }

  @Test
  void malformedQuotesAndIncompleteJsonAreNotCsv() {
    for (String text : List.of("{\"a\":1,\"b\":", "a\"b,c")) {
      assertEquals(
          DetectedFormat.UNRECOGNIZED_TEXT, FormatRecord.format(FormatDetector.classify(text)));
    }
  }

  @Test
  void boundsParserInputBeforeMaterializingOversizedYamlScalarOrJsonKey() throws Exception {
    for (String start : List.of("k: |\n  ", "{\"")) {
      AtomicInteger read = new AtomicInteger();
      byte[] prefix = start.getBytes(StandardCharsets.UTF_8);
      ByteInput input =
          () ->
              new InputStream() {
                public int read() {
                  int offset = read.getAndIncrement();
                  if (offset > 200_000)
                    fail("Recognition must stop before reading the whole oversized document");
                  return offset < prefix.length ? prefix[offset] : 'x';
                }
              };
      try (var cursor =
          decoder(AutoFormatCursor.Unit.FILE).open(input, new DecodeLimits(512, 1024))) {
        var outcome = assertInstanceOf(DecodeOutcome.Oversized.class, cursor.next());
        assertEquals(LimitKind.MAX_DOCUMENT_BYTES, outcome.limit());
        assertEquals(1024, outcome.limitValue());
      }
      assertTrue(read.get() <= 1024 + 64 * 1024);
    }
  }

  @Test
  void emitsFromLargeContainerBeforeReadingItsTail() throws Exception {
    AtomicInteger read = new AtomicInteger();
    byte[] row = "a,b\n".getBytes(StandardCharsets.UTF_8);
    ByteInput input =
        () ->
            new InputStream() {
              public int read() {
                int position = read.getAndIncrement();
                if (position > 100_000) fail("The first CSV record must not need the whole input");
                return row[position % row.length];
              }
            };
    try (var cursor = decoder(AutoFormatCursor.Unit.FILE).open(input, new DecodeLimits(32, 64))) {
      assertInstanceOf(DecodeOutcome.Decoded.class, cursor.next());
    }
    assertTrue(read.get() < 100_000);
  }

  @Test
  void bracketsDoNotSplitPlainLogLines() throws Exception {
    for (String text : List.of("[INFO] hello", "{not-json} hello", "[false]suffix")) {
      var result =
          decoder(AutoFormatCursor.Unit.FILE)
              .decodeAll(text.getBytes(StandardCharsets.UTF_8), LIMITS);
      assertTrue(result.problems().isEmpty());
      assertEquals(List.of(text), result.records().stream().map(FormatRecord::text).toList());
      assertEquals(
          DetectedFormat.UNRECOGNIZED_TEXT, FormatRecord.format(result.records().getFirst()));
    }
  }

  @Test
  void bomPreservesMultilineJsonDocumentBoundaries() throws Exception {
    String text = "\uFEFF{\n  \"a\": 1\n}\n{\"b\":2}\n";
    for (var charset :
        List.of(StandardCharsets.UTF_8, StandardCharsets.UTF_16LE, StandardCharsets.UTF_16BE)) {
      var result = decoder(AutoFormatCursor.Unit.FILE).decodeAll(text.getBytes(charset), LIMITS);
      assertTrue(result.problems().isEmpty());
      assertEquals(
          List.of(DetectedFormat.JSON_OBJECT, DetectedFormat.JSON_OBJECT),
          result.records().stream().map(FormatRecord::format).toList());
      assertEquals(
          List.of("\uFEFF{\n  \"a\": 1\n}", "{\"b\":2}"),
          result.records().stream().map(FormatRecord::text).toList());
    }
  }

  @Test
  void yamlPreambleBelongsToFirstDocument() throws Exception {
    for (String preamble :
        List.of("# intro\n", "\n\n", "%YAML 1.1\n# intro\n", "\uFEFF# intro\n")) {
      String first = preamble + "---\na: 1\n";
      String second = "---\nb: 2\n";
      var result =
          decoder(AutoFormatCursor.Unit.FILE)
              .decodeAll((first + second).getBytes(StandardCharsets.UTF_8), LIMITS);
      assertTrue(result.problems().isEmpty());
      assertEquals(
          List.of(first, second), result.records().stream().map(FormatRecord::text).toList());
      assertTrue(
          result.records().stream()
              .allMatch(record -> FormatRecord.format(record) == DetectedFormat.YAML));
    }
  }

  @Test
  void validatesSyslogTimestampStructuredDataAndCefEscapes() {
    for (String timestamp :
        List.of(
            "2003-10-11t22:14:15Z",
            "2003-10-11T22:14:15z",
            "2003-10-11T22:14:15.1234567Z",
            "2003-10-11T22:14Z")) {
      assertNotEquals(
          DetectedFormat.SYSLOG_PLAIN,
          FormatRecord.format(
              FormatDetector.classify("<34>1 " + timestamp + " host app 123 ID - message")));
    }
    String header = "<34>1 2003-10-11T22:14:15.123456+23:59 host app 123 ID ";
    assertEquals(
        DetectedFormat.SYSLOG_PLAIN,
        FormatRecord.format(FormatDetector.classify(header + "[a[b@32473 x=\"c\\q\"] message")));
    assertNotEquals(
        DetectedFormat.SYSLOG_PLAIN,
        FormatRecord.format(FormatDetector.classify(header + "[a@32473][a@32473] message")));
    String cef = "CEF:0|vendor|product|1|event|name|5|";
    assertEquals(
        DetectedFormat.SYSLOG_CEF,
        FormatRecord.format(FormatDetector.classify(cef + "msg=one\\=two src=1.2.3.4")));
    assertNotEquals(
        DetectedFormat.SYSLOG_CEF,
        FormatRecord.format(FormatDetector.classify(cef + "msg=one=two")));
    assertNotEquals(
        DetectedFormat.SYSLOG_CEF,
        FormatRecord.format(FormatDetector.classify(cef + "msg=one\ntwo")));
  }

  @Test
  void explicitYamlScalarKeepsItsPreamble() throws Exception {
    for (var unit : AutoFormatCursor.Unit.values()) {
      for (String text : List.of("# intro\n---\nhello\n", "\uFEFF# intro\n---\nhello\n")) {
        var result = decoder(unit).decodeAll(text.getBytes(StandardCharsets.UTF_8), LIMITS);
        assertTrue(result.problems().isEmpty());
        assertEquals(1, result.records().size());
        assertEquals(DetectedFormat.YAML, FormatRecord.format(result.records().getFirst()));
        assertEquals(text, FormatRecord.text(result.records().getFirst()));
      }
    }
  }

  @Test
  void interruptionDoesNotBecomeCorruptOrSuccessfulPrefix() {
    for (var unit : AutoFormatCursor.Unit.values()) {
      try (var cursor =
          decoder(unit).open(ByteInput.ofBytes("a,b".getBytes(StandardCharsets.UTF_8)), LIMITS)) {
        Thread.currentThread().interrupt();
        try {
          assertThrows(SourceReadException.class, cursor::next);
        } finally {
          Thread.interrupted();
        }
      }
    }
  }
}
