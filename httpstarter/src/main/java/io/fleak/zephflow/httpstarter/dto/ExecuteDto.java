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

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Created by bolei on 3/4/25 */
public interface ExecuteDto {

  @Data
  @NoArgsConstructor
  @Builder
  @AllArgsConstructor
  class Request {
    int byteCount;
    @Valid @NotNull List<RecordFleakData> inputRecords;
  }

  @Data
  @NoArgsConstructor
  @Builder
  @AllArgsConstructor
  class Response {
    String workflowId;
    WorkflowOutput output;
  }

  @Data
  @NoArgsConstructor
  @Builder
  @AllArgsConstructor
  class WorkflowOutput {
    @Builder.Default Map<String, List<RecordFleakData>> outputEvents = new HashMap<>();
    @Builder.Default Map<String, Map<String, List<RecordFleakData>>> outputByStep = new HashMap<>();
    @Builder.Default Map<String, Map<String, List<ErrorOutput>>> errorByStep = new HashMap<>();
  }
}
