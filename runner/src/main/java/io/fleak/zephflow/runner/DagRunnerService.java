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
package io.fleak.zephflow.runner;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.runner.dag.Dag;
import io.fleak.zephflow.runner.dag.Edge;
import io.fleak.zephflow.runner.dag.Node;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Created by bolei on 4/8/25 */
public class DagRunnerService {

  private final DagCompiler dagCompiler;
  private final MetricClientProvider metricClientProvider;
  private final Map<String, String> basicTags;

  public DagRunnerService(
      DagCompiler dagCompiler,
      MetricClientProvider metricClientProvider,
      Map<String, String> basicTags) {
    this.dagCompiler = dagCompiler;
    this.metricClientProvider = metricClientProvider;
    this.basicTags = basicTags;
  }

  public NoSourceDagRunner createForApiBackend(
      List<AdjacencyListDagDefinition.DagNode> dag, JobContext jobContext) {
    Map<String, String> metricTags = new HashMap<>(basicTags);
    if (jobContext != null) {
      metricTags.putAll(jobContext.getMetricTags());
      jobContext.setMetricTags(metricTags);
    }
    AdjacencyListDagDefinition dagDefinition =
        AdjacencyListDagDefinition.builder().jobContext(jobContext).dag(dag).build();
    Dag<OperatorCommand> compiledDag = dagCompiler.compile(dagDefinition, false);
    List<Edge> incomingEdges = new ArrayList<>();
    for (Node<OperatorCommand> node : compiledDag.getEntryNodes()) {
      if (node.getNodeContent() instanceof SourceCommand) {
        throw new IllegalArgumentException(
            "http backend doesn't support source function node in the dag. Found:"
                + node.getNodeContent().commandName());
      }
      incomingEdges.add(Edge.builder().from("http_input").to(node.getId()).build());
    }
    DagRunCounters counters =
        DagRunCounters.createPipelineCounters(metricClientProvider, metricTags);
    return new NoSourceDagRunner(incomingEdges, compiledDag, metricClientProvider, counters, false);
  }
}
