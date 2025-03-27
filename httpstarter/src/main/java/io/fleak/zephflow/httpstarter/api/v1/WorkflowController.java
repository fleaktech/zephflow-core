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

import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static io.fleak.zephflow.runner.Constants.HTTP_STARTER_WORKFLOW_CONTROLLER_PATH;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.httpstarter.dto.WorkflowDto;
import io.fleak.zephflow.runner.*;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.runner.dag.Dag;
import io.fleak.zephflow.runner.dag.Edge;
import io.fleak.zephflow.runner.dag.Node;
import jakarta.validation.Valid;
import java.util.ArrayList;
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
@RequestMapping(HTTP_STARTER_WORKFLOW_CONTROLLER_PATH)
public class WorkflowController {

  private static final JobContext DEFAULT_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(Map.of(METRIC_TAG_SERVICE, "http_endpoint", METRIC_TAG_ENV, "env"))
          .build();

  private final DagCompiler dagCompiler;
  private final ConcurrentHashMap<
          String, Pair<List<AdjacencyListDagDefinition.DagNode>, NoSourceDagRunner>>
      dagMap;

  @Autowired
  public WorkflowController(
      DagCompiler dagCompiler,
      ConcurrentHashMap<String, Pair<List<AdjacencyListDagDefinition.DagNode>, NoSourceDagRunner>>
          dagMap) {
    this.dagCompiler = dagCompiler;
    this.dagMap = dagMap;
  }

  @PostMapping
  public WorkflowDto.Response createWorkflow(@Valid @RequestBody WorkflowDto.Request request) {
    AdjacencyListDagDefinition dagDefinition =
        AdjacencyListDagDefinition.builder()
            .jobContext(DEFAULT_JOB_CONTEXT)
            .dag(request.getDag())
            .build();
    String id = generateRandomHash();
    Dag<OperatorCommand> compiledDag;
    try {
      compiledDag = dagCompiler.compile(dagDefinition, false);
    } catch (Exception e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "failed to compile dag");
    }
    List<Edge> incomingEdges = new ArrayList<>();
    for (Node<OperatorCommand> node : compiledDag.getEntryNodes()) {
      if (node.getNodeContent() instanceof SourceCommand) {
        throw new ResponseStatusException(
            HttpStatus.BAD_REQUEST,
            "http backend doesn't support source function node in the dag. Found:"
                + node.getNodeContent().commandName());
      }
      incomingEdges.add(Edge.builder().from("http_input").to(node.getId()).build());
    }
    DagRunCounters counters =
        DagRunCounters.createPipelineCounters(
            new MetricClientProvider.NoopMetricClientProvider(),
            Map.of(METRIC_TAG_SERVICE, "default_service", METRIC_TAG_ENV, "default_env"));
    NoSourceDagRunner noSourceDagRunner =
        new NoSourceDagRunner(incomingEdges, compiledDag, counters, false);
    dagMap.put(id, Pair.of(request.getDag(), noSourceDagRunner));
    return WorkflowDto.Response.builder().id(id).dag(request.getDag()).build();
  }

  @GetMapping
  public List<WorkflowDto.Response> getWorkflows() {
    return dagMap.entrySet().stream()
        .map(e -> WorkflowDto.Response.builder().id(e.getKey()).dag(e.getValue().getKey()).build())
        .toList();
  }

  @DeleteMapping("/{workflowId}")
  public void deleteWorkflow(@PathVariable("workflowId") String workflowId) {
    var dagPair = dagMap.get(workflowId);
    if (dagPair == null) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "workflow not found");
    }
    dagMap.remove(workflowId);
  }
}
