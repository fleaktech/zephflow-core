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
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class S3DecodingTest {

  static Stream<Arguments> legacyMapping() {
    return Stream.of(
        Arguments.of(EncodingType.JSON_OBJECT, new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT)),
        Arguments.of(EncodingType.JSON_ARRAY, new JsonFormatConfig(JsonFormatConfig.Mode.ARRAY)),
        Arguments.of(
            EncodingType.JSON_OBJECT_LINE, new JsonFormatConfig(JsonFormatConfig.Mode.LINES)),
        Arguments.of(
            EncodingType.STRING_LINE, new TextFormatConfig(null, new FramingConfig.Line(false))),
        Arguments.of(EncodingType.TEXT, new TextFormatConfig(null, new FramingConfig.Whole())),
        Arguments.of(
            EncodingType.CSV,
            new CsvFormatConfig(null, null, null, null, null, null, null, true, null)),
        Arguments.of(EncodingType.XML, new XmlFormatConfig(XmlFormatConfig.Mode.DOCUMENT, null)));
  }

  @ParameterizedTest
  @MethodSource("legacyMapping")
  void legacyEncodingsMapToTheirFormatWithAutoGzipDetection(
      EncodingType encodingType, FormatConfig format) {
    assertEquals(
        new DecodingConfig(List.of(DecompressionCodec.AUTO), format),
        new S3Decoding(encodingType, null, null, false).decodingConfig());
  }

  @Test
  void forcedGzipMapsToGzip() {
    assertEquals(
        new DecodingConfig(
            List.of(DecompressionCodec.GZIP), new JsonFormatConfig(JsonFormatConfig.Mode.LINES)),
        new S3Decoding(EncodingType.JSON_OBJECT_LINE, CompressionType.GZIP, null, false)
            .decodingConfig());
  }

  @Test
  void decodingIsUsedAsGiven() {
    DecodingConfig decoding =
        new DecodingConfig(List.of(DecompressionCodec.ZSTD), new ParquetFormatConfig());
    assertSame(decoding, new S3Decoding(null, null, decoding, true).decodingConfig());
  }

  @Test
  void aNestedDecodingParsesFromJson() {
    DecodingConfig parsed =
        JsonUtils.fromJsonString(
            "{\"compression\":\"zstd\",\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}",
            DecodingConfig.class);
    assertEquals(
        new DecodingConfig(
            List.of(DecompressionCodec.ZSTD),
            new TextFormatConfig(null, new FramingConfig.Line(null))),
        parsed);
  }

  @Test
  void neitherEncodingTypeNorDecodingIsRejected() {
    assertEquals(
        "exactly one of encodingType and decoding must be set",
        assertThrows(
                IllegalArgumentException.class,
                () -> new S3Decoding(null, null, null, false).decodingConfig())
            .getMessage());
  }

  @Test
  void bothEncodingTypeAndDecodingAreRejected() {
    DecodingConfig decoding =
        new DecodingConfig(null, new JsonFormatConfig(JsonFormatConfig.Mode.LINES));
    assertEquals(
        "exactly one of encodingType and decoding must be set",
        assertThrows(
                IllegalArgumentException.class,
                () -> new S3Decoding(EncodingType.CSV, null, decoding, false).decodingConfig())
            .getMessage());
  }

  @Test
  void compressionTypeWithDecodingIsRejected() {
    DecodingConfig decoding =
        new DecodingConfig(null, new JsonFormatConfig(JsonFormatConfig.Mode.LINES));
    assertEquals(
        "compressionType is only allowed with encodingType; decoding carries its own compression",
        assertThrows(
                IllegalArgumentException.class,
                () -> new S3Decoding(null, CompressionType.GZIP, decoding, false).decodingConfig())
            .getMessage());
  }

  @Test
  void parquetEncodingTypeIsRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new S3Decoding(EncodingType.PARQUET, null, null, false).decodingConfig());
  }
}
