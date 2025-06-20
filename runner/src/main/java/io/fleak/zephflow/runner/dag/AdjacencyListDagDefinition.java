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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.utils.YamlUtils;
import java.util.ArrayList;
import java.util.List;
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
    private String config;
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
}
