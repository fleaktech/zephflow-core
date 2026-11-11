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

import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.FramingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import java.util.List;
import org.junit.jupiter.api.Test;

class SourceDecodingTest {

  private static final DecodingConfig PARQUET =
      new DecodingConfig(List.of(DecompressionCodec.AUTO), new ParquetFormatConfig());

  @Test
  void decodingIsUsedAsGiven() {
    SourceDecoding decoding = new SourceDecoding(PARQUET);

    assertEquals(
        List.of(PARQUET, "parquet"), List.of(decoding.decodingConfig(), decoding.formatName()));
  }

  @Test
  void missingDecodingIsRejected() {
    assertEquals(
        "decoding is required",
        assertThrows(IllegalArgumentException.class, new SourceDecoding(null)::decodingConfig)
            .getMessage());
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
