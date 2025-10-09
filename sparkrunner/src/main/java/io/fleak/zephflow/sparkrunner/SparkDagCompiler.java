package io.fleak.zephflow.sparkrunner;

import io.fleak.zephflow.lib.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.lib.dag.compile.DagCompiler;
import io.fleak.zephflow.sparkrunner.processors.SparkCommandProcessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;

/** Created by bolei on 10/8/25 */
@Slf4j
public class SparkDagCompiler
    extends DagCompiler<List<DataStreamWriter<Row>>, SparkCommandProcessor> {
  protected SparkDagCompiler(Map<String, SparkCommandProcessor> commandRegistryMap) {
    super(commandRegistryMap);
  }

  @Override
  public List<DataStreamWriter<Row>> compile(
      AdjacencyListDagDefinition adjacencyListDagDefinition, boolean checkConnected) {

    SparkSession spark = SparkSession.builder().appName("Zephflow Spark Job").getOrCreate();

    Map<String, Dataset<Row>> processedDataFrames = new HashMap<>();
    List<DataStreamWriter<Row>> configuredWriters = new ArrayList<>();

    AdjacencyListDagDefinition.SortedDag sortedDag = adjacencyListDagDefinition.topologicalSort();
    for (AdjacencyListDagDefinition.DagNode node : sortedDag.sortedNodes()) {
      log.info("Compiling node: {} ({})", node.getId(), node.getCommandName());

      List<String> parentIds = sortedDag.parentMap().get(node.getId());

      Map<String, Dataset<Row>> parentDfs = new HashMap<>();
      for (String parentId : parentIds) {
        parentDfs.put(parentId, processedDataFrames.get(parentId));
      }

      SparkCommandProcessor processor = commandRegistryMap.get(node.getCommandName());
      Object result =
          processor.process(parentDfs, node.getConfig(), spark); // Pass config map directly

      if (result instanceof Dataset) {
        //noinspection unchecked
        processedDataFrames.put(node.getId(), (Dataset<Row>) result);
      } else if (result instanceof DataStreamWriter) {
        //noinspection unchecked
        configuredWriters.add((DataStreamWriter<Row>) result);
      } else {
        log.error("SparkCommandProcessor returns unknown result: {}", result);
        throw new IllegalStateException("unknown result: " + result);
      }
    }
    return configuredWriters;
  }
}
