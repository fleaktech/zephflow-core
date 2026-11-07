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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.lib.parser.ParserConfigs.DispatchConfig;
import io.fleak.zephflow.lib.parser.ParserConfigs.ParserConfig;
import io.fleak.zephflow.lib.parser.extractions.GrokExtractionConfig;
import io.fleak.zephflow.lib.parser.extractions.JsonExtractionConfig;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ParserConfigValidatorTest {
  private final ParserConfigValidator validator = new ParserConfigValidator();

  private static ParserConfig json(String outputFieldName) {
    return ParserConfig.builder()
        .targetField("__raw__")
        .extractionConfig(new JsonExtractionConfig(outputFieldName))
        .build();
  }

  @Test
  void jsonExtractionWithoutOutputFieldNameIsRefusedInsteadOfFailingEveryEvent() {
    IllegalArgumentException refused =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(json(null), "parse", null));
    assertEquals("json extraction needs an outputFieldName", refused.getMessage());
  }

  @Test
  void jsonExtractionWithABlankOutputFieldNameIsRefused() {
    assertThrows(
        IllegalArgumentException.class, () -> validator.validateConfig(json(" "), "parse", null));
  }

  @Test
  void jsonExtractionNestedInADispatchIsCheckedToo() {
    ParserConfig config =
        ParserConfig.builder()
            .targetField("__raw__")
            .extractionConfig(
                GrokExtractionConfig.builder().grokExpression("%{GREEDYDATA:x}").build())
            .dispatchConfig(
                DispatchConfig.builder()
                    .dispatchField("x")
                    .dispatchMap(Map.of("a", json(null)))
                    .defaultConfig(json("parsed"))
                    .build())
            .build();

    assertThrows(
        IllegalArgumentException.class, () -> validator.validateConfig(config, "parse", null));
  }

  @Test
  void jsonExtractionWithAnOutputFieldNameIsAccepted() {
    assertDoesNotThrow(() -> validator.validateConfig(json("parsed"), "parse", null));
  }
}
