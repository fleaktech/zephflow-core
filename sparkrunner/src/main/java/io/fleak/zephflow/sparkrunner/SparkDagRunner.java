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
package io.fleak.zephflow.sparkrunner;

import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.SimpleSourceCommand;
import io.fleak.zephflow.lib.dag.*;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.runner.NoSourceDagRunner;
import io.fleak.zephflow.runner.ZephflowDagCompiler;
import io.fleak.zephflow.sparkrunner.source.SparkSourceExecutor;
import io.fleak.zephflow.sparkrunner.source.SparkSourceExecutorRegistry;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

/**
 * Dynamic Spark job executor that runs DAG-defined workflows.
 *
 * <p>This is NOT a compiler - it's a runtime executor that dynamically executes Spark jobs based on
 * DAG definitions. The DAG YAML defines the job behavior at runtime.
 *
 * <p>Execution flow: Source → UDF Transform → Multi-Sink
 */
@Slf4j
@Builder
public class SparkDagRunner {

  @NonNull private final ZephflowDagCompiler zephflowDagCompiler;

  @NonNull private final SparkSession spark;

  @NonNull private final MetricClientProvider metricClientProvider;

  @NonNull private final String jobId;

  @NonNull @Builder.Default
  private final SparkDagProcessor.Config config = SparkDagProcessor.Config.builder().build();

  /**
   * Execute a complete Spark job defined by the DAG.
   *
   * @param dagDef The DAG definition (from YAML)
   * @throws Exception if execution fails
   */
  public void run(AdjacencyListDagDefinition dagDef) throws Exception {
    log.info("Starting SparkDagRunner execution");

    // 1. Compile DAG definition to OperatorCommands
    Dag<OperatorCommand> compiledDag = zephflowDagCompiler.compile(dagDef, true);
    log.info("Compiled DAG:%n {}", dagDef);

    // 2. Split DAG: source nodes | rest (intermediate + sinks)
    Pair<Dag<OperatorCommand>, Dag<OperatorCommand>> split =
        Dag.splitEntryNodesAndRest(compiledDag);
    Dag<OperatorCommand> sourceDag = split.getLeft();
    Dag<OperatorCommand> restDag = split.getRight();

    // 3. Extract sink nodes from restDag (nodes with no outgoing edges)
    List<OperatorCommand> sinkNodes =
        restDag.getNodes().stream()
            .filter(node -> restDag.downstreamEdges(node.getId()).isEmpty())
            .map(Node::getNodeContent)
            .toList();

    // 4. Extract edges from source and get JobContext
    List<Edge> edgesFromSource = sourceDag.getEdges();
    NodesEdgesDagDefinition nodesEdgesDef =
        NodesEdgesDagDefinition.fromAdjacencyListDagDefinition(dagDef);
    var jobContext = nodesEdgesDef.getJobContext();

    log.info(
        "Split DAG - Source nodes: {}, Sink nodes: {}, Intermediate nodes: {}, Edges from source: {}",
        sourceDag.getNodes().size(),
        sinkNodes.size(),
        restDag.getNodes().size() - sinkNodes.size(),
        edgesFromSource.size());

    // 5. Create processing pipeline (intermediate DAG without source)
    NoSourceDagRunner dagRunner = new NoSourceDagRunner(edgesFromSource, restDag, jobContext);
    SparkDagProcessor processor =
        SparkDagProcessor.builder().dagRunner(dagRunner).config(config).build();

    // 6. Execute Spark job pipeline
    Dataset<Row> sourceData = executeSource(sourceDag);
    Dataset<Row> processedData = processor.process(sourceData);
    executeSinks(processedData, sinkNodes);

    log.info("SparkDagRunner execution completed");
  }

  /**
   * Execute source nodes to produce initial Dataset.
   *
   * @param sourceDag DAG containing only source nodes
   * @return Dataset of input events
   */
  private Dataset<Row> executeSource(Dag<OperatorCommand> sourceDag) throws Exception {
    List<Node<OperatorCommand>> sourceNodes = new ArrayList<>(sourceDag.getNodes());
    SimpleSourceCommand<SerializedEvent> sourceCommand = getSourceCommand(sourceNodes);
    log.info("Executing source node: {}", sourceCommand.commandName());

    SparkSourceExecutor executor =
        SparkSourceExecutorRegistry.SOURCE_EXECUTOR_MAP.get(sourceCommand.commandName());
    if (executor == null) {
      throw new IllegalArgumentException(
          "Cannot run source command in spark: " + sourceCommand.commandName());
    }
    return executor.execute(sourceCommand, spark);
  }

  private static @NotNull SimpleSourceCommand<SerializedEvent> getSourceCommand(
      List<Node<OperatorCommand>> sourceNodes) {

    if (sourceNodes.isEmpty()) {
      throw new IllegalArgumentException("No source nodes provided.");
    }
    if (sourceNodes.size() > 1) {
      throw new IllegalArgumentException(
          "Multiple source nodes not yet supported. Found: " + sourceNodes.size());
    }

    Node<OperatorCommand> sourceNode = sourceNodes.get(0);
    OperatorCommand command = sourceNode.getNodeContent();
    if (!(command instanceof SimpleSourceCommand)) {
      throw new IllegalArgumentException(
          "Source node does not contain SourceCommand: " + command.getClass().getName());
    }

    //noinspection unchecked
    return (SimpleSourceCommand<SerializedEvent>) command;
  }

  /**
   * Execute sink nodes to write processed data to destinations.
   *
   * <p>TODO: Implement multi-destination sink routing logic.
   *
   * @param processedData Dataset with OUTPUT_EVENT_SCHEMA (nodeId, data)
   * @param sinkNodes List of sink command nodes
   */
  private void executeSinks(Dataset<Row> processedData, List<OperatorCommand> sinkNodes) {
    log.warn("executeSinks() not yet implemented - data not written");
    log.info("Found {} sink nodes to route data to", sinkNodes.size());
    // TODO: Implement sink execution
    // - For each sink node:
    //   - Filter processedData by nodeId matching sink node
    //   - Route to appropriate destination (Kafka, file, etc.)
    //   - Execute sink command to write data
  }
}
