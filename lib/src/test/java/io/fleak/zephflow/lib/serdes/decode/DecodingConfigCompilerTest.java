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

import static io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfigCompiler;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import org.junit.jupiter.api.Test;

class DecodingConfigCompilerTest {

  @Test
  void compressionAcceptsASingleNameOrAList() throws Exception {
    assertEquals(
        new DecodingConfig(
            List.of(DecompressionCodec.AUTO), new JsonFormatConfig(JsonFormatConfig.Mode.LINES)),
        JsonUtils.OBJECT_MAPPER.readValue(
            "{\"compression\":\"auto\",\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}",
            DecodingConfig.class));
    assertEquals(
        new DecodingConfig(
            List.of(DecompressionCodec.GZIP), new JsonFormatConfig(JsonFormatConfig.Mode.ARRAY)),
        JsonUtils.OBJECT_MAPPER.readValue(
            "{\"compression\":[\"GZIP\"],\"format\":{\"type\":\"json\",\"mode\":\"Array\"}}",
            DecodingConfig.class));
  }

  @Test
  void configErrorsAreReportedAtCompileTime() {
    for (String bad :
        List.of(
            "{}",
            "{\"format\":{\"type\":\"json\"}}",
            "{\"compression\":[\"auto\",\"gzip\"],\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}",
            "{\"format\":{\"type\":\"text\"}}",
            "{\"format\":{\"type\":\"text\",\"charset\":\"UTF-16\",\"framing\":{\"type\":\"line\"}}}",
            "{\"format\":{\"type\":\"text\",\"charset\":\"nope\",\"framing\":{\"type\":\"line\"}}}")) {
      assertThrows(IllegalArgumentException.class, () -> decoder(bad), bad);
    }
    assertThrows(IllegalArgumentException.class, () -> DecodingConfigCompiler.compile(null));
  }

  @Test
  void unknownNamesFailToParse() {
    for (String bad :
        List.of(
            "{\"compression\":\"zip\",\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}",
            "{\"format\":{\"type\":\"json\",\"mode\":\"nope\"}}",
            "{\"format\":{\"type\":\"csvx\"}}")) {
      UncheckedIOException thrown =
          assertThrows(UncheckedIOException.class, () -> decoder(bad), bad);
      assertTrue(thrown.getCause() instanceof JsonMappingException, bad);
    }
  }

  @Test
  void openReadsNothingUntilNext() {
    RecordDecoder decoder = decoder("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}");
    ByteInput failing =
        () -> {
          throw new IOException("unreachable");
        };
    try (RecordCursor cursor = decoder.open(failing, DecodeLimits.UNLIMITED)) {
      assertThrows(SourceReadException.class, cursor::next);
    }
  }

  @Test
  void readFailureIsNotFollowedByACleanEnd() throws Exception {
    RecordDecoder decoder = decoder("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}");
    try (RecordCursor cursor =
        decoder.open(failingAt(utf8("{\"a\":1}\n{\"b\":2}\n"), 10), DecodeLimits.UNLIMITED)) {
      assertEquals(new Ok(json("{\"a\":1}"), 1, new RawSpan(0, 7)), view(cursor.next()));
      assertThrows(SourceReadException.class, cursor::next);
      assertThrows(SourceReadException.class, cursor::next);
    }
  }

  @Test
  void zeroByteReadsAreRetriedNotTakenAsTheEnd() {
    byte[] data = gzip("a\n", "b\n");
    ByteInput stuttering =
        () ->
            new InputStream() {
              private int pos;
              private boolean zeroNext = true;

              @Override
              public int read() {
                return pos < data.length ? data[pos++] & 0xFF : -1;
              }

              @Override
              public int read(byte[] b, int off, int len) {
                zeroNext = !zeroNext;
                if (!zeroNext) {
                  return 0;
                }
                if (pos >= data.length) {
                  return -1;
                }
                b[off] = data[pos++];
                return 1;
              }
            };
    assertEquals(
        List.of(new Ok(raw("a"), 1, new RawSpan(0, 1)), new Ok(raw("b"), 2, new RawSpan(2, 1))),
        decode(
            decoder(
                "{\"compression\":\"gzip\",\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}"),
            stuttering,
            DecodeLimits.UNLIMITED));
  }

  @Test
  void aStreamThatOnlyReturnsZeroBytesFailsAsARead() {
    ByteInput stuck =
        () ->
            new InputStream() {
              @Override
              public int read() {
                return 0;
              }

              @Override
              public int read(byte[] b, int off, int len) {
                return 0;
              }
            };
    assertEquals(
        List.of(new Threw("SourceReadException")),
        decode(
            decoder("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}"),
            stuck,
            DecodeLimits.UNLIMITED));
  }

  @Test
  void decodeAllSplitsRecordsAndProblems() {
    DecodeResult result =
        decoder("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}")
            .decodeAll(utf8("{\"a\":1}\nbad\n"), DecodeLimits.UNLIMITED);
    assertEquals(List.of(json("{\"a\":1}")), result.records());
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "bad",
                2,
                new RawSpan(8, 3),
                "JsonParseException: Unrecognized token 'bad': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')",
                false)),
        result.problems().stream().map(DecodeTestSupport::view).toList());
  }
}
