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
package io.fleak.zephflow.lib.serdes.des.csv;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonResource;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

/** Created by bolei on 9/17/24 */
class CsvTypedDeserializerTest {

  @Test
  void deserializeToMultipleTypedEvent() throws Exception {
    byte[] raw;
    try (InputStream is = this.getClass().getResourceAsStream("/serdes/test_csv_input.csv")) {
      Preconditions.checkNotNull(is);
      raw = IOUtils.toByteArray(is);
    }
    CsvTypedDeserializer converter = new CsvTypedDeserializer();
    List<Map<String, Object>> events = converter.deserializeToMultipleTypedEvent(raw);
    List<JsonNode> jsonList = events.stream().map(JsonUtils::convertToJsonNode).toList();
    List<ObjectNode> expected =
        fromJsonResource("/serdes/test_csv_expected_output.json", new TypeReference<>() {});
    assertEquals(expected, jsonList);
  }
}
