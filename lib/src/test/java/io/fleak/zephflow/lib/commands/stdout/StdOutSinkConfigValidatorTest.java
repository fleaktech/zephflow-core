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
package io.fleak.zephflow.lib.commands.stdout;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.encode.EncodingConfig;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import org.junit.jupiter.api.Test;

class StdOutSinkConfigValidatorTest {

  private final StdOutSinkConfigValidator validator = new StdOutSinkConfigValidator();

  @Test
  void validateConfig_unsupportedEncodingType() {
    StdOutDto.Config config =
        StdOutDto.Config.builder().encodingType(EncodingType.STRING_LINE).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertTrue(exception.getMessage().contains("Unsupported serialization encoding type"));
  }

  @Test
  void validateConfig_allSupportedEncodingTypes() {
    for (EncodingType type : SerializerFactory.SUPPORTED_ENCODING_TYPES) {
      StdOutDto.Config config = StdOutDto.Config.builder().encodingType(type).build();

      assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", null));
    }
  }

  private static EncodingConfig encoding(String json) {
    return fromJsonString(json, EncodingConfig.class);
  }

  @Test
  void validateConfig_encodingAlone() {
    StdOutDto.Config config =
        StdOutDto.Config.builder()
            .encoding(encoding("{\"format\":{\"type\":\"json\",\"mode\":\"object\"}}"))
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", null));
  }

  @Test
  void validateConfig_encodingWithEncodingType() {
    StdOutDto.Config config =
        StdOutDto.Config.builder()
            .encodingType(EncodingType.JSON_OBJECT)
            .encoding(encoding("{\"format\":{\"type\":\"json\",\"mode\":\"object\"}}"))
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertEquals("stdout: encoding cannot be combined with encodingType", exception.getMessage());
  }

  @Test
  void validateConfig_encodingInAFormatItCannotWrite() {
    StdOutDto.Config config =
        StdOutDto.Config.builder().encoding(encoding("{\"format\":{\"type\":\"kv\"}}")).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertEquals("stdout cannot write kv", exception.getMessage());
  }

  @Test
  void validateConfig_neitherEncodingNorEncodingType() {
    StdOutDto.Config config = StdOutDto.Config.builder().build();

    NullPointerException exception =
        assertThrows(
            NullPointerException.class, () -> validator.validateConfig(config, "test-node", null));
    assertEquals("no encoding type is provided", exception.getMessage());
  }
}
