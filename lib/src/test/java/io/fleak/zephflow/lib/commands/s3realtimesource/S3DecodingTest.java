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

import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import org.junit.jupiter.api.Test;

class S3DecodingTest {

  @Test
  void decodingIsUsedAsGiven() {
    DecodingConfig decoding =
        new DecodingConfig(List.of(DecompressionCodec.ZSTD), new ParquetFormatConfig());
    assertSame(decoding, new S3Decoding(decoding, true).decodingConfig());
  }

  @Test
  void missingDecodingIsRejected() {
    assertEquals(
        "decoding is required",
        assertThrows(
                IllegalArgumentException.class, () -> new S3Decoding(null, false).decodingConfig())
            .getMessage());
  }

  @Test
  void theFormatNameIsTheDecodingsFormatType() {
    List<String> types =
        List.of("avro", "csv", "json", "otlp", "parquet", "protobuf", "text", "xml", "yaml");
    assertEquals(
        types,
        types.stream()
            .map(
                type ->
                    new S3Decoding(
                            JsonUtils.fromJsonString(
                                "{\"format\":{\"type\":\"" + type + "\"}}", DecodingConfig.class),
                            false)
                        .formatName())
            .toList());
  }
}
