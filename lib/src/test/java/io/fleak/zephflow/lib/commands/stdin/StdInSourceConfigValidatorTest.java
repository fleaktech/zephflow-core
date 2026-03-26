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
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
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
}
