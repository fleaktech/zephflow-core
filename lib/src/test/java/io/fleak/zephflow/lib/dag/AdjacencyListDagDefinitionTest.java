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
package io.fleak.zephflow.lib.dag;

import static io.fleak.zephflow.lib.utils.YamlUtils.fromYamlResource;
import static io.fleak.zephflow.lib.utils.YamlUtils.fromYamlString;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Created by bolei on 3/21/25 */
class AdjacencyListDagDefinitionTest {

  @Test
  void testToString() throws IOException {
    AdjacencyListDagDefinition def =
        fromYamlResource("/dags/test_dag.yml", new TypeReference<>() {});
    String defStr = def.toString();
    AdjacencyListDagDefinition actual = fromYamlString(defStr, new TypeReference<>() {});
    assertEquals(def, actual);
  }
}
