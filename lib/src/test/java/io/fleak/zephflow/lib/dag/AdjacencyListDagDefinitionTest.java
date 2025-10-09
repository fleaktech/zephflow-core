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
import io.fleak.zephflow.lib.dag.AdjacencyListDagDefinition.DagNode;
import io.fleak.zephflow.lib.dag.AdjacencyListDagDefinition.SortedDag;
import java.io.IOException;
import java.util.List;
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

  @Test
  void testTopologicalSort_linearDag() {
    DagNode node1 = DagNode.builder().id("1").commandName("cmd1").outputs(List.of("2")).build();
    DagNode node2 = DagNode.builder().id("2").commandName("cmd2").outputs(List.of("3")).build();
    DagNode node3 = DagNode.builder().id("3").commandName("cmd3").outputs(List.of()).build();

    AdjacencyListDagDefinition dag =
        AdjacencyListDagDefinition.builder().dag(List.of(node1, node2, node3)).build();

    SortedDag sorted = dag.topologicalSort();

    assertEquals(3, sorted.sortedNodes().size());
    assertEquals("1", sorted.sortedNodes().get(0).getId());
    assertEquals("2", sorted.sortedNodes().get(1).getId());
    assertEquals("3", sorted.sortedNodes().get(2).getId());

    assertEquals(List.of(), sorted.parentMap().get("1"));
    assertEquals(List.of("1"), sorted.parentMap().get("2"));
    assertEquals(List.of("2"), sorted.parentMap().get("3"));
  }

  @Test
  void testTopologicalSort_multipleRoots() {
    DagNode node1 = DagNode.builder().id("1").commandName("cmd1").outputs(List.of("3")).build();
    DagNode node2 = DagNode.builder().id("2").commandName("cmd2").outputs(List.of("3")).build();
    DagNode node3 = DagNode.builder().id("3").commandName("cmd3").outputs(List.of()).build();

    AdjacencyListDagDefinition dag =
        AdjacencyListDagDefinition.builder().dag(List.of(node1, node2, node3)).build();

    SortedDag sorted = dag.topologicalSort();

    assertEquals(3, sorted.sortedNodes().size());
    assertEquals("3", sorted.sortedNodes().get(2).getId());

    assertEquals(List.of(), sorted.parentMap().get("1"));
    assertEquals(List.of(), sorted.parentMap().get("2"));
    assertEquals(2, sorted.parentMap().get("3").size());
    assertTrue(sorted.parentMap().get("3").contains("1"));
    assertTrue(sorted.parentMap().get("3").contains("2"));
  }

  @Test
  void testTopologicalSort_complexDag() {
    DagNode node1 =
        DagNode.builder().id("1").commandName("cmd1").outputs(List.of("2", "3")).build();
    DagNode node2 = DagNode.builder().id("2").commandName("cmd2").outputs(List.of("4")).build();
    DagNode node3 = DagNode.builder().id("3").commandName("cmd3").outputs(List.of("4")).build();
    DagNode node4 = DagNode.builder().id("4").commandName("cmd4").outputs(List.of()).build();

    AdjacencyListDagDefinition dag =
        AdjacencyListDagDefinition.builder().dag(List.of(node1, node2, node3, node4)).build();

    SortedDag sorted = dag.topologicalSort();

    assertEquals(4, sorted.sortedNodes().size());
    assertEquals("1", sorted.sortedNodes().get(0).getId());
    assertEquals("4", sorted.sortedNodes().get(3).getId());

    assertEquals(List.of(), sorted.parentMap().get("1"));
    assertEquals(List.of("1"), sorted.parentMap().get("2"));
    assertEquals(List.of("1"), sorted.parentMap().get("3"));
    assertEquals(2, sorted.parentMap().get("4").size());
    assertTrue(sorted.parentMap().get("4").contains("2"));
    assertTrue(sorted.parentMap().get("4").contains("3"));
  }

  @Test
  void testTopologicalSort_emptyDag() {
    AdjacencyListDagDefinition dag = AdjacencyListDagDefinition.builder().dag(List.of()).build();

    SortedDag sorted = dag.topologicalSort();

    assertEquals(0, sorted.sortedNodes().size());
    assertEquals(0, sorted.parentMap().size());
  }

  @Test
  void testTopologicalSort_nullDag() {
    AdjacencyListDagDefinition dag = AdjacencyListDagDefinition.builder().dag(null).build();

    SortedDag sorted = dag.topologicalSort();

    assertEquals(0, sorted.sortedNodes().size());
    assertEquals(0, sorted.parentMap().size());
  }

  @Test
  void testTopologicalSort_cycleDetection() {
    DagNode node1 = DagNode.builder().id("1").commandName("cmd1").outputs(List.of("2")).build();
    DagNode node2 = DagNode.builder().id("2").commandName("cmd2").outputs(List.of("3")).build();
    DagNode node3 = DagNode.builder().id("3").commandName("cmd3").outputs(List.of("1")).build();

    AdjacencyListDagDefinition dag =
        AdjacencyListDagDefinition.builder().dag(List.of(node1, node2, node3)).build();

    IllegalStateException exception =
        assertThrows(IllegalStateException.class, dag::topologicalSort);
    assertEquals("DAG contains a cycle", exception.getMessage());
  }

  @Test
  void testTopologicalSort_singleNode() {
    DagNode node1 = DagNode.builder().id("1").commandName("cmd1").outputs(List.of()).build();

    AdjacencyListDagDefinition dag =
        AdjacencyListDagDefinition.builder().dag(List.of(node1)).build();

    SortedDag sorted = dag.topologicalSort();

    assertEquals(1, sorted.sortedNodes().size());
    assertEquals("1", sorted.sortedNodes().get(0).getId());
    assertEquals(List.of(), sorted.parentMap().get("1"));
  }
}
