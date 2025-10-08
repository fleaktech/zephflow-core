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
import java.util.HashMap;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Created by bolei on 3/5/25 */
@Data
@NoArgsConstructor
@Builder
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodesEdgesDagDefinition {
  private JobContext jobContext;
  private Dag<RawDagNode> dag;

  public static NodesEdgesDagDefinition fromAdjacencyListDagDefinition(
      AdjacencyListDagDefinition adjacencyListDagDefinition) {

    JobContext jc = adjacencyListDagDefinition.getJobContext();
    if (jc.getMetricTags() == null) {
      jc.setMetricTags(new HashMap<>());
    }
    if (jc.getOtherProperties() == null) {
      jc.setOtherProperties(new HashMap<>());
    }
    return NodesEdgesDagDefinition.builder()
        .jobContext(jc)
        .dag(Dag.fromAdjacencyList(adjacencyListDagDefinition.getDag()))
        .build();
  }
}
