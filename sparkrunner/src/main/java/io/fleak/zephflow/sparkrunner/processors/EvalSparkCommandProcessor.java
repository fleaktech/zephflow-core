package io.fleak.zephflow.sparkrunner.processors;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/** Created by bolei on 10/9/25 */
public class EvalSparkCommandProcessor implements SparkCommandProcessor {
  @Override
  public Object process(
      Map<String, Dataset<Row>> parentDataFrames, Map<String, Object> config, SparkSession spark) {
    String expression = (String) config.get("expression");

    return null;
  }
}
