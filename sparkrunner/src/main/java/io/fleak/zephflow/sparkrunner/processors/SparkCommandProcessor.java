package io.fleak.zephflow.sparkrunner.processors;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Created by bolei on 10/9/25 */
public interface SparkCommandProcessor {
  /**
   * Processes a node from the DAG.
   *
   * @return An Object, which can be a Dataset<Row> for transformations or a DataStreamWriter<Row>
   *     for sinks.
   */
  Object process(
      Map<String, Dataset<Row>> parentDataFrames, Map<String, Object> config, SparkSession spark);
}
