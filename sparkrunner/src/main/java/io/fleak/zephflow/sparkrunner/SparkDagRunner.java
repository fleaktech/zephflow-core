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

import static java.util.Collections.emptyList;

import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.SimpleSourceCommand;
import io.fleak.zephflow.lib.dag.*;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.runner.NoSourceDagRunner;
import io.fleak.zephflow.runner.ZephflowDagCompiler;
import io.fleak.zephflow.sparkrunner.sink.SparkSinkExecutor;
import io.fleak.zephflow.sparkrunner.sink.SparkSinkExecutorRegistry;
import io.fleak.zephflow.sparkrunner.source.SparkSourceExecutor;
import io.fleak.zephflow.sparkrunner.source.SparkSourceExecutorRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
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
    List<Node<OperatorCommand>> sinkNodeList =
        restDag.getNodes().stream()
            .filter(node -> node.getNodeContent() instanceof ScalarSinkCommand)
            .toList();

    // 4. Extract edges pointing to sink nodes from compiled DAG
    List<String> sinkNodeIds = sinkNodeList.stream().map(Node::getId).toList();
    List<Edge> edgesToSinks =
        compiledDag.getEdges().stream().filter(edge -> sinkNodeIds.contains(edge.getTo())).toList();

    // 5. Extract edges from source and get JobContext
    List<Edge> edgesFromSource = sourceDag.getEdges();
    NodesEdgesDagDefinition nodesEdgesDef =
        NodesEdgesDagDefinition.fromAdjacencyListDagDefinition(dagDef);
    var jobContext = nodesEdgesDef.getJobContext();

    log.info(
        "Split DAG - Source nodes: {}, Sink nodes: {}, Intermediate nodes: {}, Edges from source: {}, Edges to sinks: {}",
        sourceDag.getNodes().size(),
        sinkNodeList.size(),
        restDag.getNodes().size() - sinkNodeList.size(),
        edgesFromSource.size(),
        edgesToSinks.size());

    // 6. Create processing pipeline (intermediate DAG without source)
    NoSourceDagRunner dagRunner = new NoSourceDagRunner(edgesFromSource, restDag, jobContext);
    SparkDagProcessor processor =
        SparkDagProcessor.builder().dagRunner(dagRunner).config(config).build();

    // 7. Execute Spark job pipeline
    Dataset<Row> sourceData = executeSource(sourceDag);
    Dataset<Row> processedData = processor.process(sourceData);
    executeSinks(processedData, sinkNodeList, edgesToSinks);

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
   * <p>Routes data to multiple sinks based on nodeId field in OUTPUT_EVENT_SCHEMA.
   *
   * @param processedData Dataset with OUTPUT_EVENT_SCHEMA (nodeId, data)
   * @param sinkNodeList List of sink nodes (with IDs)
   * @param edgesToSinks List of edges pointing to sink nodes (from processing nodes to sinks)
   */
  private void executeSinks(
      Dataset<Row> processedData, List<Node<OperatorCommand>> sinkNodeList, List<Edge> edgesToSinks)
      throws Exception {
    log.info(
        "Executing {} sink nodes with {} incoming edges", sinkNodeList.size(), edgesToSinks.size());

    // Step 1: Build processingNodeId -> [sinkIds] map directly from edges
    Map<String, List<String>> nodeToSinksMap = new HashMap<>();
    for (Edge edge : edgesToSinks) {
      nodeToSinksMap.computeIfAbsent(edge.getFrom(), k -> new ArrayList<>()).add(edge.getTo());
    }

    // Build sinkId -> OperatorCommand map
    Map<String, OperatorCommand> sinkNodeIdToCommand = new HashMap<>();
    for (Node<OperatorCommand> node : sinkNodeList) {
      sinkNodeIdToCommand.put(node.getId(), node.getNodeContent());
    }

    // Step 2: Broadcast and create UDF
    Broadcast<Map<String, List<String>>> bNodeToSinksMap;
    try (JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext())) {
      bNodeToSinksMap = jsc.broadcast(nodeToSinksMap);
    }

    UserDefinedFunction getSinksUDF =
        functions.udf(
            (String nodeId) -> {
              Map<String, List<String>> localMap = bNodeToSinksMap.value();
              return localMap.getOrDefault(nodeId, emptyList());
            },
            DataTypes.createArrayType(DataTypes.StringType));

    spark.udf().register("getSinks", getSinksUDF);

    // Step 3: Add sinks column and explode to duplicate rows
    Dataset<Row> withSinks =
        processedData.withColumn("sinks", functions.callUDF("getSinks", functions.col("nodeId")));

    Dataset<Row> dataToRoute =
        withSinks
            .withColumn("sinkId", functions.explode(functions.col("sinks")))
            .drop("sinks", "nodeId");

    // Step 4: Cache and write loop
    try {
      dataToRoute.cache();
      long rowCount = dataToRoute.count();
      log.info("Cached {} rows for routing to sinks", rowCount);

      for (Map.Entry<String, OperatorCommand> entry : sinkNodeIdToCommand.entrySet()) {
        String sinkId = entry.getKey();
        OperatorCommand sink = entry.getValue();

        log.info("Writing data for sink: {}", sinkId);

        // Filter cached data for this sink
        Dataset<Row> sinkData =
            dataToRoute.filter(functions.col("sinkId").equalTo(sinkId)).drop("sinkId");
        // Get executor
        SparkSinkExecutor executor =
            SparkSinkExecutorRegistry.SINK_EXECUTOR_MAP.get(sink.commandName());

        if (executor == null) {
          throw new IllegalArgumentException(
              String.format("No Spark sink executor found for '%s'", sink.commandName()));
        }

        // Write using Spark-native sink
        executor.execute(sinkData, sink, spark);
        log.info("Successfully wrote to sink '{}'", sinkId);
      }

      log.info("All sinks executed successfully");
    } finally {
      // Clean up cache regardless of success or failure
      dataToRoute.unpersist();
      log.info("Unpersisted routing data");
    }
  }
}
