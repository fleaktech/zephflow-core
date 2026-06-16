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
package io.fleak.zephflow.lib.commands.eval;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.*;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DictRemoveFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testDictRemove() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "dict1", Map.of("a", 1, "b", 2),
                "dict2", Map.of("c", 3, "d", 4)));

    try {
      FleakData result = evaluateExpression("dict_remove($[\"dict1\"], \"b\")", testData);
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> removed = (Map<String, Object>) result.unwrap();
      assertTrue(removed.containsKey("a"));
      assertFalse(removed.containsKey("b"));
      assertEquals(1L, removed.get("a"));
    } catch (Exception e) {
      fail("dict_remove failed: " + e.getMessage());
    }

    try {
      FleakData result = evaluateExpression("dict_remove($[\"dict2\"], \"c\", \"d\")", testData);
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> removed = (Map<String, Object>) result.unwrap();
      assertTrue(removed.isEmpty());
    } catch (Exception e) {
      fail("dict_remove with multiple keys failed: " + e.getMessage());
    }

    try {
      FleakData result = evaluateExpression("dict_remove($[\"dict1\"], \"nonexistent\")", testData);
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> removed = (Map<String, Object>) result.unwrap();
      assertEquals(2, removed.size());
      assertTrue(removed.containsKey("a"));
      assertTrue(removed.containsKey("b"));
    } catch (Exception e) {
      fail("dict_remove with non-existent key failed: " + e.getMessage());
    }
  }

  @Test
  public void testDictRemoveNullHandling() {
    FleakData testData =
        FleakData.wrap(Map.of("dict1", Map.of("a", 1, "b", 2, "c", 3), "key", "b"));

    testFunctionExecution(testData, "dict_remove(null, \"a\")", null);
    testFunctionExecution(testData, "dict_remove($.nonexistent, \"a\")", null);
    testFunctionExecution(
        testData, "dict_remove($.dict1, null)", Map.of("a", 1L, "b", 2L, "c", 3L));
    testFunctionExecution(testData, "dict_remove($.dict1, \"b\", null, \"c\")", Map.of("a", 1L));
    testFunctionExecution(testData, "dict_remove($.dict1, $.key)", Map.of("a", 1L, "c", 3L));
  }
}
