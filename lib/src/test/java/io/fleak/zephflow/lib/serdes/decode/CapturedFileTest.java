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

import io.fleak.zephflow.lib.serdes.decode.config.*;
import io.fleak.zephflow.lib.serdes.decode.json.JsonEncoding;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CapturedFileTest {
  private static final DecodeLimits LIMITS = new DecodeLimits(1024 * 1024, 2 * 1024 * 1024);

  @Test
  void admittedEmptyLineKeepsItsTerminatorAcrossCharsetsAndCompression() throws Exception {
    for (String ending : List.of("\n", "\r\n")) {
      for (String encoding : List.of("UTF-8", "UTF-16LE", "UTF-16BE")) {
        for (boolean compressed : List.of(false, true)) {
          for (String admitted : List.of(ending, "first" + ending + ending)) {
            byte[] bytes = (admitted + "tail" + ending).getBytes(Charset.forName(encoding));
            if (compressed) {
              ByteArrayOutputStream output = new ByteArrayOutputStream();
              try (var gzip = new GZIPOutputStream(output)) {
                gzip.write(bytes);
              }
              bytes = output.toByteArray();
            }
            var compression = List.of(DecompressionCodec.AUTO);
            int records = admitted.startsWith("first") ? 2 : 1;
            long boundary;
            try (var cursor =
                DecodingConfigCompiler.compile(
                        new DecodingConfig(compression, new AutoFormatConfig()))
                    .open(ByteInput.ofBytes(bytes), LIMITS)) {
              for (int index = 0; index < records; index++)
                assertInstanceOf(DecodeOutcome.Decoded.class, cursor.next());
              boundary = cursor.normalizedEndOffset();
            }
            var config =
                new DecodingConfig(
                    compression, new TextFormatConfig(encoding, new FramingConfig.Line(false)));
            var result =
                DecodingConfigCompiler.compileCapturedFile(config, boundary)
                    .decodeAll(bytes, LIMITS);
            assertTrue(result.problems().isEmpty(), result.problems().toString());
            assertEquals(records, result.records().size(), encoding + " compressed=" + compressed);
            assertEquals("", result.records().getLast().getPayload().get("__raw__").unwrap());
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTF-8", "UTF-16LE", "UTF-16BE", "UTF-32LE", "UTF-32BE"})
  void preservesExactOriginalEncodingAndBomAtUnicodeBoundary(String encoding) throws Exception {
    Charset charset = Charset.forName(encoding);
    for (String mark : List.of("", "\uFEFF")) {
      byte[] prefix = (mark + "{\"value\":\"ż😀\"}\n").getBytes(charset);
      byte[] original = (mark + "{\"value\":\"ż😀\"}\n{\"tail\":true}").getBytes(charset);
      long normalizedLength;
      try (var normalized = JsonEncoding.strictUtf8(new ByteArrayInputStream(prefix))) {
        normalizedLength = normalized.readAllBytes().length;
      }
      try (var bounded =
          JsonEncoding.originalPrefix(new ByteArrayInputStream(original), normalizedLength)) {
        assertArrayEquals(prefix, bounded.readAllBytes(), encoding + " BOM=" + !mark.isEmpty());
      }
    }
  }

  @Test
  void configuredCsvKeepsHeaderAndQuotedNewlineButExcludesUncapturedTail() throws Exception {
    String admitted = "name,value\n\"two\nlines\",2";
    byte[] original = (admitted + "\ntail,3\n").getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (var gzip = new GZIPOutputStream(compressed)) {
      gzip.write(original);
    }
    var config =
        new DecodingConfig(
            List.of(DecompressionCodec.GZIP),
            new CsvFormatConfig(null, null, null, true, null, null, null, null, null));
    var result =
        DecodingConfigCompiler.compileCapturedFile(
                config, admitted.getBytes(StandardCharsets.UTF_8).length)
            .decodeAll(compressed.toByteArray(), LIMITS);
    assertTrue(result.problems().isEmpty(), result.problems().toString());
    assertEquals(
        List.of(Map.of("name", "two\nlines", "value", "2")),
        result.records().stream().map(record -> record.unwrap()).toList());
  }

  @Test
  void capturedArrayMayProduceMoreRecordsThanAutomaticFrameCount() throws Exception {
    String array = "[{\"n\":1},{\"n\":2},{\"n\":3}]";
    var config = new DecodingConfig(List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.ARRAY));
    var result =
        DecodingConfigCompiler.compileCapturedFile(config, array.length())
            .decodeAll(array.getBytes(StandardCharsets.UTF_8), LIMITS);
    assertTrue(result.problems().isEmpty());
    assertEquals(3, result.records().size());
  }
}
