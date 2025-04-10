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
package io.fleak.zephflow.httpstarter.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Created by bolei on 3/4/25 */
@JsonIgnoreProperties(ignoreUnknown = true)
public interface WorkflowDto {
  @Data
  @NoArgsConstructor
  @Builder
  @AllArgsConstructor
  class Request {
    @NotNull @Valid private List<AdjacencyListDagDefinition.DagNode> dag;
  }

  @Data
  @NoArgsConstructor
  @Builder
  @AllArgsConstructor
  class Response {
    String id;
    @NotNull @Valid private List<AdjacencyListDagDefinition.DagNode> dag;
  }
}
