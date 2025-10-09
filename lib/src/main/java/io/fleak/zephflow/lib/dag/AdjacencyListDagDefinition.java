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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.utils.YamlUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Created by bolei on 2/27/25 */
@Data
@NoArgsConstructor
@Builder
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class AdjacencyListDagDefinition {
  private JobContext jobContext;
  private List<DagNode> dag;

  @Data
  @NoArgsConstructor
  @Builder
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DagNode {
    private String id;
    private String commandName;
    private Map<String, Object> config;
    @Builder.Default private List<String> outputs = new ArrayList<>();

    public DagNode duplicate() {
      var o = Stream.ofNullable(outputs).flatMap(List::stream).toList();
      return DagNode.builder()
          .id(id)
          .commandName(commandName)
          .config(config)
          .outputs(new ArrayList<>(o)) // Ensure mutability
          .build();
    }
  }

  @Override
  public String toString() {
    return YamlUtils.toYamlString(this);
  }

  public SortedDag topologicalSort() {
    if (dag == null || dag.isEmpty()) {
      return new SortedDag(List.of(), Map.of());
    }

    Map<String, List<String>> parentMap = new HashMap<>();
    Map<String, Integer> inDegree = new HashMap<>();
    Map<String, DagNode> nodeMap = new HashMap<>();

    for (DagNode node : dag) {
      nodeMap.put(node.getId(), node);
      parentMap.put(node.getId(), new ArrayList<>());
      inDegree.put(node.getId(), 0);
    }

    for (DagNode node : dag) {
      for (String childId : node.getOutputs()) {
        parentMap.get(childId).add(node.getId());
        inDegree.put(childId, inDegree.get(childId) + 1);
      }
    }

    Queue<DagNode> queue = new LinkedList<>();
    for (DagNode node : dag) {
      if (inDegree.get(node.getId()) == 0) {
        queue.offer(node);
      }
    }

    List<DagNode> sortedNodes = new ArrayList<>();
    while (!queue.isEmpty()) {
      DagNode current = queue.poll();
      sortedNodes.add(current);

      for (String childId : current.getOutputs()) {
        int newInDegree = inDegree.get(childId) - 1;
        inDegree.put(childId, newInDegree);

        if (newInDegree == 0) {
          queue.offer(nodeMap.get(childId));
        }
      }
    }

    if (sortedNodes.size() != dag.size()) {
      throw new IllegalStateException("DAG contains a cycle");
    }

    return new SortedDag(sortedNodes, parentMap);
  }

  public record SortedDag(
      List<AdjacencyListDagDefinition.DagNode> sortedNodes,
      Map<String, List<String>> parentMap // Key: node ID, Value: List of parent IDs
      ) {}
}
