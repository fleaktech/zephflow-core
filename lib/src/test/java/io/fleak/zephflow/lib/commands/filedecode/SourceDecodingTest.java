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

import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.config.CsvFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FramingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.XmlFormatConfig;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SourceDecodingTest {

  private static final DecodingConfig PARQUET =
      new DecodingConfig(List.of(DecompressionCodec.AUTO), new ParquetFormatConfig());

  @Test
  void legacyEncodingTypesMapToDecodingConfigurations() {
    Map<EncodingType, FormatConfig> expected =
        Map.of(
            EncodingType.JSON_OBJECT, new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT),
            EncodingType.JSON_ARRAY, new JsonFormatConfig(JsonFormatConfig.Mode.ARRAY),
            EncodingType.JSON_OBJECT_LINE, new JsonFormatConfig(JsonFormatConfig.Mode.LINES),
            EncodingType.STRING_LINE, new TextFormatConfig(null, new FramingConfig.Line(false)),
            EncodingType.TEXT, new TextFormatConfig(null, new FramingConfig.Whole()),
            EncodingType.CSV,
                new CsvFormatConfig(null, null, null, null, null, null, null, true, null),
            EncodingType.XML, new XmlFormatConfig(XmlFormatConfig.Mode.DOCUMENT, null));

    assertEquals(
        expected.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new DecodingConfig(List.of(DecompressionCodec.AUTO), e.getValue()))),
        expected.keySet().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(), t -> new SourceDecoding(t, null, null).decodingConfig())));
  }

  @Test
  void gzipIsTheOnlyLegacyCompressionSetting() {
    assertEquals(
        new DecodingConfig(
            List.of(DecompressionCodec.GZIP), new JsonFormatConfig(JsonFormatConfig.Mode.LINES)),
        new SourceDecoding(EncodingType.JSON_OBJECT_LINE, CompressionType.GZIP, null)
            .decodingConfig());
  }

  @Test
  void decodingIsUsedAsGiven() {
    SourceDecoding decoding = new SourceDecoding(null, null, PARQUET);

    assertEquals(
        List.of(PARQUET, "parquet"), List.of(decoding.decodingConfig(), decoding.formatName()));
  }

  @Test
  void invalidCombinationsAreRejected() {
    assertEquals(
        List.of(
            "exactly one of encodingType and decoding must be set",
            "exactly one of encodingType and decoding must be set",
            "compressionType is only allowed with encodingType; decoding carries its own"
                + " compression",
            "Unsupported deserialization encoding type: PARQUET"),
        Arrays.stream(
                new SourceDecoding[] {
                  new SourceDecoding(null, null, null),
                  new SourceDecoding(EncodingType.CSV, null, PARQUET),
                  new SourceDecoding(null, CompressionType.GZIP, PARQUET),
                  new SourceDecoding(EncodingType.PARQUET, null, null)
                })
            .map(
                d ->
                    assertThrows(IllegalArgumentException.class, d::decodingConfig)
                        .getMessage()
                        .split("\\. Supported types")[0])
            .toList());
  }

  @Test
  void onlyMultilineTextFramingIsMultiline() {
    assertEquals(
        List.of(true, false, false),
        List.of(
                new TextFormatConfig(null, new FramingConfig.Multiline("^\\S", null, null)),
                new TextFormatConfig(null, new FramingConfig.Line(false)),
                new JsonFormatConfig(JsonFormatConfig.Mode.LINES))
            .stream()
            .map(SourceDecoding::multiline)
            .toList());
  }
}
