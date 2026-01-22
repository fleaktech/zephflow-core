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
package io.fleak.zephflow.runner.dag;

import static io.fleak.zephflow.lib.utils.YamlUtils.fromYamlResource;
import static io.fleak.zephflow.lib.utils.YamlUtils.fromYamlString;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 3/21/25 */
class AdjacencyListDagDefinitionTest {

  @Test
  void testToString() throws IOException {
    AdjacencyListDagDefinition def = fromYamlResource("/test_dag.yml", new TypeReference<>() {});
    String defStr = def.toString();
    AdjacencyListDagDefinition actual = fromYamlString(defStr, new TypeReference<>() {});
    assertEquals(def, actual);
  }

  @SuppressWarnings("unchecked")
  @Test
  void duplicate_shouldDeepCopyConfig() {
    Map<String, Object> nestedConfig = new HashMap<>();
    nestedConfig.put("nestedKey", "nestedValue");

    Map<String, Object> config = new HashMap<>();
    config.put("key1", "value1");
    config.put("nested", nestedConfig);

    var original =
        AdjacencyListDagDefinition.DagNode.builder()
            .id("node1")
            .commandName("testCommand")
            .config(config)
            .outputs(List.of("node2"))
            .build();

    var duplicate = original.duplicate();

    // Mutate the duplicate's config
    duplicate.getConfig().put("key1", "mutatedValue");
    ((Map<String, Object>) duplicate.getConfig().get("nested")).put("nestedKey", "mutatedNested");

    // Original should be unchanged
    assertEquals("value1", original.getConfig().get("key1"));
    assertEquals(
        "nestedValue", ((Map<String, Object>) original.getConfig().get("nested")).get("nestedKey"));
  }

  @Test
  void duplicate_shouldHandleNullConfig() {
    var original =
        AdjacencyListDagDefinition.DagNode.builder()
            .id("node1")
            .commandName("testCommand")
            .config(null)
            .outputs(List.of("node2"))
            .build();

    var duplicate = original.duplicate();

    assertNull(duplicate.getConfig());
  }
}
