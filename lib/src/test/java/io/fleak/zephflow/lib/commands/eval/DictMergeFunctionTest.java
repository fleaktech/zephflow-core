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

class DictMergeFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testDictMerge() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "dict1", Map.of("a", 1, "b", 2),
                "dict2", Map.of("c", 3, "d", 4)));

    try {
      FleakData result = evaluateExpression("dict_merge($[\"dict1\"], $[\"dict2\"])", testData);
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> merged = (Map<String, Object>) result.unwrap();
      assertTrue(merged.containsKey("a") && merged.containsKey("c"));
    } catch (Exception e) {
      fail("dict_merge failed: " + e.getMessage());
    }
  }
}
