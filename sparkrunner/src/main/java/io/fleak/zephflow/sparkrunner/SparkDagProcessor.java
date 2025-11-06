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

import io.fleak.zephflow.runner.NoSourceDagRunner;
import java.io.Serializable;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

/**
 * High-level API for processing Spark datasets through ZephFlow DAGs.
 *
 * <p>Example usage:
 *
 * <pre>
 * NoSourceDagRunner dagRunner = // ... compile your DAG
 * SparkDagProcessor processor = SparkDagProcessor.builder()
 *     .dagRunner(dagRunner)
 *     .batchSize(1000)
 *     .build();
 *
 * Dataset&lt;Row&gt; inputDataset = // ... load your input data
 * Dataset&lt;Row&gt; outputDataset = processor.process(inputDataset);
 * </pre>
 */
@Builder
public class SparkDagProcessor implements Serializable {

  /** Configuration for DAG processing behavior. */
  @Getter
  @Builder
  public static class Config implements Serializable {
    /** Number of records to process in each batch. Default: 1000 */
    @Builder.Default private int batchSize = 1000;
  }

  @NonNull private final NoSourceDagRunner dagRunner;

  @NonNull @Builder.Default private final Config config = Config.builder().build();

  /**
   * Process a dataset of input events through the DAG.
   *
   * @param inputDataset Dataset with schema matching SparkSchemas.INPUT_EVENT_SCHEMA
   * @return Dataset with schema matching SparkSchemas.OUTPUT_EVENT_SCHEMA
   * @throws IllegalArgumentException if input schema doesn't match expected schema
   */
  public Dataset<Row> process(Dataset<Row> inputDataset) {
    if (!SparkSchemas.INPUT_EVENT_SCHEMA.equals(inputDataset.schema())) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid input schema. Expected: %s, but got: %s",
              SparkSchemas.INPUT_EVENT_SCHEMA, inputDataset.schema()));
    }

    DagProcessorUDF udf = new DagProcessorUDF(dagRunner, config.getBatchSize());

    return inputDataset.mapPartitions(udf, Encoders.row(SparkSchemas.OUTPUT_EVENT_SCHEMA));
  }
}
