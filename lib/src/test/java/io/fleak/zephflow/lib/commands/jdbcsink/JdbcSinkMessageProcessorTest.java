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
package io.fleak.zephflow.lib.commands.jdbcsink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JdbcSinkMessageProcessorTest {

  @Test
  void testPreprocess() throws Exception {
    JdbcSinkMessageProcessor processor = new JdbcSinkMessageProcessor();

    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test"));
    Map<String, Object> result = processor.preprocess(record, System.currentTimeMillis());

    assertNotNull(result);
    assertEquals(1.0, ((Number) result.get("id")).doubleValue());
    assertEquals("test", result.get("name"));
  }

  @Test
  void testPreprocessWithNullValues() throws Exception {
    JdbcSinkMessageProcessor processor = new JdbcSinkMessageProcessor();

    Map<String, FleakData> payload = new java.util.HashMap<>();
    payload.put("id", FleakData.wrap(1));
    payload.put("name", null);
    RecordFleakData record = new RecordFleakData(payload);

    Map<String, Object> result = processor.preprocess(record, System.currentTimeMillis());

    assertNotNull(result);
    assertNull(result.get("name"));
  }
}
