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
import io.fleak.zephflow.api.KeyedStatefulCommand;
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
import java.util.Objects;
import lombok.NonNull;

/** Created by bolei on 4/8/25 */
public class DagRunnerService {
  private static final String SYNC_INPUT_NODE_NAME = "sync_input";

  private final DagCompiler dagCompiler;
  private final MetricClientProvider metricClientProvider;

  public DagRunnerService(DagCompiler dagCompiler, MetricClientProvider metricClientProvider) {
    this.dagCompiler = dagCompiler;
    this.metricClientProvider = metricClientProvider;
  }

  public NoSourceDagRunner createForApiBackend(
      List<AdjacencyListDagDefinition.DagNode> dag, @NonNull JobContext jobContext) {
    return create(dag, jobContext, false);
  }

  /**
   * Builds a runner for one bounded test input that is discarded afterwards, so keyed-stateful
   * commands are allowed: their state cannot leak into another request, and the caller declares end
   * of input via {@link NoSourceDagRunner#run(List, String, NoSourceDagRunner.DagRunConfig,
   * boolean)}.
   */
  public NoSourceDagRunner createForTestRun(
      List<AdjacencyListDagDefinition.DagNode> dag, @NonNull JobContext jobContext) {
    return create(dag, jobContext, true);
  }

  /**
   * Compiles a prepared bounded graph. Source and skipped nodes must already be replaced by safe
   * boundaries; actual source constructors and validators are never used by this entry point.
   */
  public NoSourceDagRunner createForBoundedRun(
      List<AdjacencyListDagDefinition.DagNode> dag,
      JobContext jobContext,
      BoundedDefinition definition) {
    Objects.requireNonNull(definition);
    var properties = new HashMap<>(jobContext.getOtherProperties());
    properties.put(JobContext.FLAG_BOUNDED_MODE, true);
    properties.put(JobContext.FLAG_TEST_MODE, false);
    JobContext boundedContext =
        new JobContext(properties, jobContext.getMetricTags(), jobContext.getLogLevel(), null);
    for (var node : dag) {
      var factory = dagCompiler.commandFactoryMap().get(node.getCommandName());
      if (factory != null && factory.commandType() == io.fleak.zephflow.api.CommandType.SOURCE) {
        throw new IllegalArgumentException(
            "Stored execution requires a source OUTPUT boundary: " + node.getId());
      }
      if (definition.boundaries().containsKey(node.getId())
          && !"noop".equals(node.getCommandName())) {
        throw new IllegalArgumentException(
            "Bounded boundary must be a prepared noop: " + node.getId());
      }
    }
    AdjacencyListDagDefinition dagDefinition =
        AdjacencyListDagDefinition.builder().jobContext(boundedContext).dag(dag).build();
    Dag<OperatorCommand> compiled = dagCompiler.compile(dagDefinition, false);
    definition.boundaries().keySet().forEach(compiled::lookupNode);
    definition.externalNodeIds().forEach(compiled::lookupNode);
    for (var node : compiled.getNodes()) {
      if (node.getNodeContent() instanceof SourceCommand) {
        throw new IllegalArgumentException(
            "Stored execution requires a source OUTPUT boundary: " + node.getId());
      }
    }
    DagRunCounters counters =
        DagRunCounters.createPipelineCounters(metricClientProvider, boundedContext.getMetricTags());
    return new NoSourceDagRunner(
        List.of(), compiled, metricClientProvider, counters, false, definition);
  }

  private NoSourceDagRunner create(
      List<AdjacencyListDagDefinition.DagNode> dag,
      JobContext jobContext,
      boolean allowKeyedStateful) {
    AdjacencyListDagDefinition dagDefinition =
        AdjacencyListDagDefinition.builder().jobContext(jobContext).dag(dag).build();
    Dag<OperatorCommand> compiledDag = dagCompiler.compile(dagDefinition, false);
    // The api backend has no flush scheduler and reuses the runner across requests, so keyed
    // reduction state (windowed aggregation, throttle, sample, ...) can't fire on time and would
    // leak across requests. Reject these keyed-stateful commands at build time unless the runner is
    // a one-shot test run.
    for (Node<OperatorCommand> node : compiledDag.getNodes()) {
      if (!allowKeyedStateful && node.getNodeContent() instanceof KeyedStatefulCommand) {
        throw new IllegalArgumentException(
            "api backend doesn't support keyed stateful command node in the dag (windowed, "
                + "throttle or sample); it requires a streaming source pipeline. Found: "
                + node.getNodeContent().commandName());
      }
    }
    List<Edge> incomingEdges = new ArrayList<>();
    for (Node<OperatorCommand> node : compiledDag.getEntryNodes()) {
      if (node.getNodeContent() instanceof SourceCommand) {
        throw new IllegalArgumentException(
            "api backend doesn't support source function node in the dag. Found:"
                + node.getNodeContent().commandName());
      }
      incomingEdges.add(Edge.builder().from(SYNC_INPUT_NODE_NAME).to(node.getId()).build());
    }
    DagRunCounters counters =
        DagRunCounters.createPipelineCounters(
            metricClientProvider, dagDefinition.getJobContext().getMetricTags());
    return new NoSourceDagRunner(incomingEdges, compiledDag, metricClientProvider, counters, false);
  }
}
