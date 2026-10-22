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
package io.fleak.zephflow.lib.commands.stdin;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.FramingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import java.util.List;
import org.junit.jupiter.api.Test;

class StdInSourceConfigValidatorTest {

  private final StdInSourceConfigValidator validator = new StdInSourceConfigValidator();

  @Test
  void validateConfig_unsupportedEncodingType() {
    StdInSourceDto.Config config =
        StdInSourceDto.Config.builder().encodingType(EncodingType.PARQUET).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertTrue(exception.getMessage().contains("Unsupported deserialization encoding type"));
  }

  @Test
  void validateConfig_allSupportedEncodingTypes() {
    for (EncodingType type : DeserializerFactory.SUPPORTED_ENCODING_TYPES) {
      StdInSourceDto.Config config = StdInSourceDto.Config.builder().encodingType(type).build();

      assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", null));
    }
  }

  @Test
  void neitherEncodingTypeNorDecodingIsRejected() {
    StdInSourceDto.Config config = StdInSourceDto.Config.builder().build();

    assertEquals("exactly one of encodingType and decoding must be set", decodingRejection(config));
  }

  @Test
  void decodingReplacesEncodingType() {
    StdInSourceDto.Config config =
        StdInSourceDto.Config.builder()
            .decoding(
                new DecodingConfig(List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.LINES)))
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", null));
  }

  @Test
  void encodingTypeAndDecodingTogetherAreRejected() {
    StdInSourceDto.Config config =
        StdInSourceDto.Config.builder()
            .encodingType(EncodingType.JSON_OBJECT)
            .decoding(new DecodingConfig(List.of(), new ParquetFormatConfig()))
            .build();

    assertEquals("exactly one of encodingType and decoding must be set", decodingRejection(config));
  }

  @Test
  void aDecodingTheCompilerRejectsFailsValidation() {
    StdInSourceDto.Config config =
        StdInSourceDto.Config.builder()
            .decoding(
                new DecodingConfig(List.of(DecompressionCodec.GZIP), new ParquetFormatConfig()))
            .build();

    assertEquals(
        "parquet cannot be combined with compression; its pages carry their own",
        decodingRejection(config));
  }

  private String decodingRejection(StdInSourceDto.Config config) {
    return assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null))
        .getMessage();
  }

  @Test
  void multilineDecodingIsRejected() {
    StdInSourceDto.Config config =
        StdInSourceDto.Config.builder()
            .decoding(
                new DecodingConfig(
                    List.of(),
                    new TextFormatConfig(null, new FramingConfig.Multiline("^\\S", null, null))))
            .build();

    assertEquals(
        "stdin reads one message per line, so decoding cannot use multiline framing",
        decodingRejection(config));
  }
}
