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
package io.fleak.zephflow.sparkrunner.source;

import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.SimpleSourceCommand;
import io.fleak.zephflow.sparkrunner.SparkDataConverter;
import io.fleak.zephflow.sparkrunner.SparkSchemas;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Fallback executor that reuses existing ZephFlow SourceCommand implementations.
 *
 * <p>This executor runs the SourceCommand in a background thread and buffers the emitted
 * RecordFleakData batches into a queue, then converts them to a Spark Dataset.
 *
 * <p>Note: This is suitable for BATCH sources or small STREAMING sources. For large streaming
 * sources, consider using NativeSparkSourceExecutor instead.
 */
@Slf4j
@Builder
public record BufferedSparkSourceExecutor(
    @NonNull MetricClientProvider metricClientProvider, @NonNull String jobId)
    implements SparkSourceExecutor {

  @Override
  public Dataset<Row> execute(SimpleSourceCommand sourceCommand, SparkSession spark)
      throws Exception {
    log.info(
        "Executing source command '{}' using BufferedSparkSourceExecutor",
        sourceCommand.commandName());

    List<Row> allRows = new ArrayList<>();

    try {
      sourceCommand.initialize(metricClientProvider);
      sourceCommand.execute(
          jobId,
          new SourceEventAcceptor() {
            @Override
            public void accept(List<RecordFleakData> batch) {
              log.info(
                  "Collected {} records from source '{}'",
                  batch.size(),
                  sourceCommand.commandName());
              List<Row> rows = SparkDataConverter.recordsToRows(batch);
              allRows.addAll(rows);
            }

            @Override
            public void terminate() throws IOException {
              sourceCommand.terminate();
              log.debug("Source command terminated");
            }
          });
    } catch (Exception e) {
      log.error("Error executing source command", e);
    }

    log.info("Collected {} records from source '{}'", allRows.size(), sourceCommand.commandName());
    return spark.createDataset(allRows, Encoders.row(SparkSchemas.INPUT_EVENT_SCHEMA));
  }
}
