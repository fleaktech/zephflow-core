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
package io.fleak.zephflow.httpstarter.api.v1;

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_ENV;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_SERVICE;
import static io.fleak.zephflow.runner.Constants.HTTP_STARTER_EXECUTION_CONTROLLER_PATH;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.httpstarter.dto.ExecuteDto;
import io.fleak.zephflow.lib.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.runner.DagResult;
import io.fleak.zephflow.runner.DagRunnerService;
import io.fleak.zephflow.runner.NoSourceDagRunner;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

/** Created by bolei on 3/4/25 */
@RestController
@RequestMapping(HTTP_STARTER_EXECUTION_CONTROLLER_PATH)
public class ExecutionController {

  private static final JobContext DEFAULT_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(Map.of(METRIC_TAG_SERVICE, "http_endpoint", METRIC_TAG_ENV, "env"))
          .build();

  private final ConcurrentHashMap<
          String, Pair<List<AdjacencyListDagDefinition.DagNode>, NoSourceDagRunner>>
      dagMap;
  private final DagRunnerService dagRunnerService;

  @Autowired
  public ExecutionController(
      ConcurrentHashMap<String, Pair<List<AdjacencyListDagDefinition.DagNode>, NoSourceDagRunner>>
          dagMap,
      DagRunnerService dagRunnerService) {
    this.dagMap = dagMap;
    this.dagRunnerService = dagRunnerService;
  }

  @PostMapping("/run/{workflowId}/batch")
  public ExecuteDto.Response execute(
      @PathVariable("workflowId") String workflowId,
      @RequestBody ExecuteDto.Request batchPayload,
      @RequestParam(defaultValue = "false", name = "includeErrorByStep") boolean includeErrorByStep,
      @RequestParam(defaultValue = "false", name = "includeOutputByStep")
          boolean includeOutputByStep) {
    var dagPair = dagMap.get(workflowId);
    if (dagPair == null) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "workflow not found");
    }
    NoSourceDagRunner noSourceDagRunner = dagPair.getValue();
    try {
      DagResult dagResult =
          noSourceDagRunner.run(
              batchPayload.getInputRecords(),
              "http_endpoint_user",
              new NoSourceDagRunner.DagRunConfig(includeErrorByStep, includeOutputByStep),
              dagRunnerService.metricClientProvider());

      return ExecuteDto.Response.builder()
          .workflowId(workflowId)
          .output(
              ExecuteDto.WorkflowOutput.builder()
                  .outputEvents(dagResult.getOutputEvents())
                  .errorByStep(dagResult.getErrorByStep())
                  .outputByStep(dagResult.getOutputByStep())
                  .build())
          .build();
    } catch (Exception e) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST,
          String.format("failed to process input request. reason: %s", e.getMessage()));
    }
  }
}
