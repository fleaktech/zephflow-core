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
package io.fleak.zephflow.lib.commands.parser;

import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.lib.parser.ParserConfigs;
import io.fleak.zephflow.lib.parser.extractions.WindowsMultilineExtractionConfig;
import java.util.List;
import org.junit.jupiter.api.Test;

class ParserConfigValidatorTest {

  private final ParserConfigValidator validator = new ParserConfigValidator();

  @Test
  void validateConfigAcceptsWindowsTimezone() {
    assertDoesNotThrow(() -> validator.validateConfig(windows("America/Chicago"), "n", null));
    assertDoesNotThrow(() -> validator.validateConfig(windows("+02:00"), "n", null));
    assertDoesNotThrow(() -> validator.validateConfig(windows(null), "n", null));
  }

  @Test
  void validateConfigRejectsUnknownWindowsTimezone() {
    ConfigurationValidationException e =
        assertThrows(
            ConfigurationValidationException.class,
            () -> validator.validateConfig(windows("America/Chicgo"), "n", null));
    assertEquals(
        List.of(
            new ConfigurationIssue(
                ConfigurationPath.of("extractionConfig", "timezone"),
                "timezone is not a recognised time zone name or offset")),
        e.issues());
  }

  private static ParserConfigs.ParserConfig windows(String timezone) {
    return ParserConfigs.ParserConfig.builder()
        .targetField(FIELD_NAME_RAW)
        .extractionConfig(WindowsMultilineExtractionConfig.builder().timezone(timezone).build())
        .build();
  }
}
