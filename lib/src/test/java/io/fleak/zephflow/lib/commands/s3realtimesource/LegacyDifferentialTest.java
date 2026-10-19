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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.utils.CompressionUtils;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Feeds the same bytes to the legacy deserializers (as the legacy S3 realtime converter used them)
 * and to {@link S3ObjectDecoder} with the legacy mapping, and pins both results: the same records
 * for valid input, and each decoding difference of the spec's section 4.2.
 */
class LegacyDifferentialTest {

  private static final long HEAP = 2048L * 1024 * 1024;
  private static final String FAILS = "FAILS";
  private static final byte[] JSON_LINES = utf8("{\"a\":1}\n{\"a\":2}\n");

  @TempDir Path spoolDir;

  /**
   * What each side gives. {@code legacy} is the records as JSON, or {@code FAILS} when the legacy
   * path failed the whole object; {@code decoded} is each outcome: {@code ok <json>}, {@code bad
   * <kind>[ terminal] <raw>}, or {@code big #<index>}.
   */
  record Observed(List<String> legacy, List<String> decoded) {}

  static Stream<Arguments> cases() throws IOException {
    return Stream.of(
        // valid input decodes the same
        same(
            "jsonObject",
            EncodingType.JSON_OBJECT,
            utf8("{\"a\":1,\"b\":\"x\"}"),
            "{\"a\":1,\"b\":\"x\"}"),
        same(
            "jsonArray",
            EncodingType.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\":2}]"),
            "{\"a\":1}",
            "{\"a\":2}"),
        same("jsonLines", EncodingType.JSON_OBJECT_LINE, JSON_LINES, "{\"a\":1}", "{\"a\":2}"),
        same(
            "stringLineKeepsBlankLines",
            EncodingType.STRING_LINE,
            utf8("x\n\ny\n"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"y\"}"),
        same("text", EncodingType.TEXT, utf8("hello\nworld"), "{\"__raw__\":\"hello\\nworld\"}"),
        same(
            "csvTrimsSpaces",
            EncodingType.CSV,
            utf8("name, age\n bob , 3\n"),
            "{\"name\":\"bob\",\"age\":\"3\"}"),
        same("xml", EncodingType.XML, utf8("<r><a>1</a></r>"), "{\"a\":\"1\"}"),
        same(
            "gzipDetected",
            EncodingType.JSON_OBJECT_LINE,
            gzip(JSON_LINES),
            "{\"a\":1}",
            "{\"a\":2}"),
        Arguments.of(
            "gzipForced",
            EncodingType.JSON_OBJECT_LINE,
            CompressionType.GZIP,
            gzip(JSON_LINES),
            HEAP,
            new Observed(
                List.of("{\"a\":1}", "{\"a\":2}"), List.of("ok {\"a\":1}", "ok {\"a\":2}"))),
        same("emptyJsonLines", EncodingType.JSON_OBJECT_LINE, new byte[0]),
        // bad data loses only itself
        legacy(
            "jsonLinesBadLine",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\nnot json\n{\"a\":3}\n"),
            List.of(FAILS),
            "ok {\"a\":1}",
            "bad MALFORMED not json",
            "ok {\"a\":3}"),
        legacy(
            "jsonLinesArrayLineFails",
            EncodingType.JSON_OBJECT_LINE,
            utf8("[{\"a\":1},{\"a\":2}]\n"),
            List.of("{\"a\":1}", "{\"a\":2}"),
            "bad MALFORMED [{\"a\":1},{\"a\":2}]"),
        legacy(
            "csvLongRowFails",
            EncodingType.CSV,
            utf8("a,b\n1,2,3\n"),
            List.of("{\"a\":\"1\",\"b\":\"2\"}"),
            "bad MALFORMED 1,2,3"),
        legacy(
            "csvShortRowFailsAlone",
            EncodingType.CSV,
            utf8("a,b\n1\n4,5\n"),
            List.of(FAILS),
            "bad MALFORMED 1",
            "ok {\"a\":\"4\",\"b\":\"5\"}"),
        legacy(
            "csvBadQuoteFailsAlone",
            EncodingType.CSV,
            utf8("a,b\n\"1\"x,2\n3,4\n"),
            List.of(FAILS),
            "bad MALFORMED \"1\"x,2",
            "ok {\"a\":\"3\",\"b\":\"4\"}"),
        legacy(
            "csvUnclosedQuoteEndsTheObject",
            EncodingType.CSV,
            utf8("a,b\n\"1,2\n"),
            List.of(FAILS),
            "bad MALFORMED terminal \"1,2\n"),
        legacy(
            "csvDuplicateHeaderEndsTheObject",
            EncodingType.CSV,
            utf8("a,a\n1,2\n"),
            List.of("{\"a\":\"2\"}"),
            "bad MALFORMED terminal a,a"),
        legacy(
            "csvEmptyHeaderIsNamed",
            EncodingType.CSV,
            utf8("a,\n1,2\n"),
            List.of(FAILS),
            "ok {\"a\":\"1\",\"_c1\":\"2\"}"),
        // byte order marks are removed
        legacy(
            "bomStringLine",
            EncodingType.STRING_LINE,
            utf8("﻿x\n"),
            List.of("{\"__raw__\":\"﻿x\"}"),
            "ok {\"__raw__\":\"x\"}"),
        legacy(
            "bomText",
            EncodingType.TEXT,
            utf8("﻿x"),
            List.of("{\"__raw__\":\"﻿x\"}"),
            "ok {\"__raw__\":\"x\"}"),
        legacy(
            "bomCsv",
            EncodingType.CSV,
            utf8("﻿a\n1\n"),
            List.of("{\"﻿a\":\"1\"}"),
            "ok {\"a\":\"1\"}"),
        legacy(
            "bomJsonLines",
            EncodingType.JSON_OBJECT_LINE,
            utf8("﻿{\"a\":1}\n"),
            List.of(FAILS),
            "ok {\"a\":1}"),
        // an empty object gives nothing
        legacy("emptyJsonObject", EncodingType.JSON_OBJECT, new byte[0], List.of(FAILS)),
        legacy("emptyJsonArray", EncodingType.JSON_ARRAY, new byte[0], List.of(FAILS)),
        legacy("emptyXml", EncodingType.XML, new byte[0], List.of(FAILS)),
        legacy("emptyText", EncodingType.TEXT, new byte[0], List.of("{\"__raw__\":\"\"}")),
        Arguments.of(
            "emptyForcedGzip",
            EncodingType.JSON_OBJECT_LINE,
            CompressionType.GZIP,
            new byte[0],
            HEAP,
            new Observed(List.of(FAILS), List.of())),
        // gzip trailing bytes
        legacy(
            "gzipTrailingBytes",
            EncodingType.JSON_OBJECT_LINE,
            concat(gzip(JSON_LINES), utf8("xx")),
            List.of("{\"a\":1}", "{\"a\":2}"),
            "ok {\"a\":1}",
            "ok {\"a\":2}",
            "bad CORRUPT_COMPRESSION terminal "),
        // xml
        legacy(
            "xmlDoctypeFails",
            EncodingType.XML,
            utf8("<!DOCTYPE r><r><a>1</a></r>"),
            List.of("{\"a\":\"1\"}"),
            "bad MALFORMED terminal <!DOCTYPE r><r><a>1</a></r>"),
        legacy(
            "xmlTrailingContentFails",
            EncodingType.XML,
            utf8("<r><a>1</a></r><s/>"),
            List.of("{\"a\":\"1\"}"),
            "bad MALFORMED terminal <r><a>1</a></r><s/>"),
        // json lines bytes and whitespace
        legacy(
            "jsonLinesInvalidUtf8Fails",
            EncodingType.JSON_OBJECT_LINE,
            concat(utf8("{\"a\":\""), new byte[] {(byte) 0xff}, utf8("\"}\n")),
            List.of("{\"a\":\"�\"}"),
            "bad MALFORMED {\"a\":\"�\"}"),
        legacy(
            "jsonLinesFormFeedLineFails",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\n\f\n{\"a\":2}\n"),
            List.of("{\"a\":1}", "{\"a\":2}"),
            "ok {\"a\":1}",
            "bad MALFORMED \f",
            "ok {\"a\":2}"),
        legacy(
            "jsonLinesUtf16Decodes",
            EncodingType.JSON_OBJECT_LINE,
            concat(
                new byte[] {(byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(StandardCharsets.UTF_16BE)),
            List.of(FAILS),
            "ok {\"a\":1}"),
        // limits are new: at a 256 KiB heap a record may be 2 KiB
        Arguments.of(
            "aRecordOverTheLimitIsOversized",
            EncodingType.STRING_LINE,
            null,
            utf8("x".repeat(3000) + "\nok\n"),
            256L * 1024,
            new Observed(
                List.of("{\"__raw__\":\"" + "x".repeat(3000) + "\"}", "{\"__raw__\":\"ok\"}"),
                List.of("big #1", "ok {\"__raw__\":\"ok\"}"))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void legacyAndDecoderResults(
      String name,
      EncodingType encodingType,
      CompressionType compressionType,
      byte[] bytes,
      long heap,
      Observed expected)
      throws SourceReadException {
    assertEquals(
        expected,
        new Observed(
            legacy(encodingType, compressionType, bytes),
            decoded(encodingType, compressionType, bytes, heap)));
  }

  @Test
  void aTruncatedGzipEmitsTheRecordsBeforeTheCutWhereLegacyFailed() throws Exception {
    List<String> lines =
        IntStream.range(0, 3000).mapToObj(i -> "{\"a\":" + i * 7919 % 100003 + "}").toList();
    byte[] whole = gzip(utf8(String.join("\n", lines) + "\n"));
    byte[] truncated = Arrays.copyOf(whole, whole.length / 2);

    List<String> decoded = decoded(EncodingType.JSON_OBJECT_LINE, null, truncated, HEAP);

    assertEquals(List.of(FAILS), legacy(EncodingType.JSON_OBJECT_LINE, null, truncated));
    int emitted = decoded.size() - 1;
    assertTrue(emitted > 0, "records before the cut: " + emitted);
    List<String> expected =
        new ArrayList<>(lines.subList(0, emitted).stream().map(l -> "ok " + l).toList());
    expected.add("bad CORRUPT_COMPRESSION terminal ");
    assertEquals(expected, decoded);
  }

  private static Arguments same(
      String name, EncodingType encodingType, byte[] bytes, String... records) {
    return Arguments.of(
        name,
        encodingType,
        null,
        bytes,
        HEAP,
        new Observed(List.of(records), Arrays.stream(records).map(r -> "ok " + r).toList()));
  }

  private static Arguments legacy(
      String name,
      EncodingType encodingType,
      byte[] bytes,
      List<String> legacy,
      String... decoded) {
    return Arguments.of(
        name, encodingType, null, bytes, HEAP, new Observed(legacy, List.of(decoded)));
  }

  /** The legacy converter's path: gzip by magic bytes or forced, then the legacy deserializer. */
  private static List<String> legacy(
      EncodingType encodingType, CompressionType compressionType, byte[] bytes) {
    try {
      byte[] body = bytes;
      boolean gzip =
          compressionType == CompressionType.GZIP
              || (compressionType == null
                  && body.length >= 2
                  && (body[0] & 0xFF) == 0x1F
                  && (body[1] & 0xFF) == 0x8B);
      if (gzip) {
        body = CompressionUtils.gunzip(body);
      }
      List<RecordFleakData> records =
          DeserializerFactory.createDeserializerFactory(encodingType)
              .createDeserializer()
              .deserialize(new SerializedEvent(null, body, null));
      return records.stream().map(r -> JsonUtils.toJsonString(r.unwrap())).toList();
    } catch (Exception e) {
      return List.of(FAILS);
    }
  }

  private List<String> decoded(
      EncodingType encodingType, CompressionType compressionType, byte[] bytes, long heap)
      throws SourceReadException {
    S3Client s3 = mock(S3Client.class);
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(
            new ResponseInputStream<>(
                GetObjectResponse.builder().contentLength((long) bytes.length).build(),
                AbortableInputStream.create(
                    new ByteArrayInputStream(bytes), mock(Abortable.class))));
    List<String> seen = new ArrayList<>();
    new S3ObjectDecoder(new S3Decoding(encodingType, compressionType, null, false), heap)
        .decode(
            s3,
            new S3ObjectRef("b", "k", "us-east-1"),
            Long.MAX_VALUE,
            spoolDir,
            outcome -> {
              seen.add(view(outcome));
              return true;
            });
    return seen;
  }

  private static String view(DecodeOutcome outcome) {
    return switch (outcome) {
      case DecodeOutcome.Decoded d -> "ok " + JsonUtils.toJsonString(d.record().unwrap());
      case DecodeOutcome.Failed f ->
          "bad "
              + f.kind()
              + (f.terminal() ? " terminal " : " ")
              + new String(f.raw(), StandardCharsets.UTF_8);
      case DecodeOutcome.Oversized o -> "big #" + o.recordIndex();
    };
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] gzip(byte[] bytes) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out.toByteArray();
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] part : parts) {
      out.writeBytes(part);
    }
    return out.toByteArray();
  }
}
