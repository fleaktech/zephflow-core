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
package io.fleak.zephflow.lib.parser.extractions;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.parser.ParserConfigs;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KvPairExtractionConfigTest {

  @Test
  @DisplayName("Should not serialize derived unescaped fields")
  void testJsonSerializationExcludesUnescapedFields() {
    KvPairExtractionConfig config =
        KvPairExtractionConfig.builder().pairSeparator("\\t").kvSeparator("=").build();
    String json = toJsonString(config);

    assertNotNull(json);
    assertFalse(json.contains("unescapedPairSeparator"));
    assertFalse(json.contains("unescapedKvSeparator"));
  }

  @Test
  @DisplayName("Should deserialize correctly without unescaped fields")
  void testJsonDeserializationRoundtrip() {
    KvPairExtractionConfig original =
        KvPairExtractionConfig.builder().pairSeparator("\\t").kvSeparator("=").build();
    String json = toJsonString(original);
    KvPairExtractionConfig deserialized = fromJsonString(json, KvPairExtractionConfig.class);

    assertNotNull(deserialized);
    assertEquals(original.getPairSeparator(), deserialized.getPairSeparator());
    assertEquals(original.getKvSeparator(), deserialized.getKvSeparator());
    assertEquals("\t", deserialized.getUnescapedPairSeparator());
  }

  @Test
  @DisplayName("Should work in ParserConfig JSON roundtrip")
  void testParserConfigJsonRoundtrip() {
    ParserConfigs.ParserConfig original =
        ParserConfigs.ParserConfig.builder()
            .targetField(FIELD_NAME_RAW)
            .extractionConfig(
                KvPairExtractionConfig.builder().pairSeparator(" ").kvSeparator("=").build())
            .build();

    String json = toJsonString(List.of(original));
    List<ParserConfigs.ParserConfig> deserialized = fromJsonString(json, new TypeReference<>() {});

    assertNotNull(deserialized);
    assertEquals(1, deserialized.size());
    KvPairExtractionConfig extractionConfig =
        (KvPairExtractionConfig) deserialized.get(0).getExtractionConfig();
    assertEquals(" ", extractionConfig.getPairSeparator());
    assertEquals("=", extractionConfig.getKvSeparator());
  }
}
