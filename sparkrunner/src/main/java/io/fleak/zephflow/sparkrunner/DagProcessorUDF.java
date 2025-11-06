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

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.runner.DagResult;
import io.fleak.zephflow.runner.DagRunCounters;
import io.fleak.zephflow.runner.NoSourceDagRunner;
import java.io.Serializable;
import java.util.*;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

/**
 * Spark UDF that processes batches of input events through a NoSourceDagRunner.
 *
 * <p>This UDF uses a streaming pattern with lazy evaluation to avoid loading entire partitions into
 * memory. Records are processed in configurable batches, with results yielded incrementally.
 */
public record DagProcessorUDF(NoSourceDagRunner noSourceDagRunner, int batchSize)
    implements MapPartitionsFunction<Row, Row>, Serializable {

  /**
   * Compact constructor to validate batch size.
   *
   * @throws IllegalArgumentException if batchSize <= 0
   */
  public DagProcessorUDF {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
    }
  }

  @Override
  public Iterator<Row> call(Iterator<Row> inputRows) {
    return new StreamingBatchIterator(inputRows);
  }

  /**
   * Lazy iterator that processes batches on-demand as Spark pulls results. Memory usage is bounded
   * to approximately 2x batchSize worth of records.
   */
  private class StreamingBatchIterator implements Iterator<Row> {
    private final Iterator<Row> inputRows;
    private Iterator<Row> currentBatchOutput = Collections.emptyIterator();

    // Created once per partition
    private transient MetricClientProvider metricProvider;
    private transient DagRunCounters dagRunCounters;

    StreamingBatchIterator(Iterator<Row> inputRows) {
      this.inputRows = inputRows;
      ensureMetricObjectsInitialized();
    }

    /** Ensures metric objects are initialized. Recreates them if null (after deserialization). */
    private void ensureMetricObjectsInitialized() {
      if (metricProvider == null) {
        metricProvider = new MetricClientProvider.NoopMetricClientProvider();
      }
      if (dagRunCounters == null) {
        dagRunCounters = DagRunCounters.createPipelineCounters(metricProvider, Map.of());
      }
    }

    @Override
    public boolean hasNext() {
      // If current batch output is exhausted, process next batch
      while (!currentBatchOutput.hasNext() && inputRows.hasNext()) {
        processBatchAndSetOutput();
      }
      return currentBatchOutput.hasNext();
    }

    @Override
    public Row next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return currentBatchOutput.next();
    }

    private void processBatchAndSetOutput() {
      List<RecordFleakData> currentBatch = new ArrayList<>(batchSize);
      while (inputRows.hasNext() && currentBatch.size() < batchSize) {
        Row inputRow = inputRows.next();
        RecordFleakData record = SparkDataConverter.inputRowToRecord(inputRow);
        currentBatch.add(record);
      }

      if (currentBatch.isEmpty()) {
        return;
      }

      ensureMetricObjectsInitialized();

      DagResult result =
          noSourceDagRunner.run(
              currentBatch,
              "spark_executor",
              new NoSourceDagRunner.DagRunConfig(false, false),
              metricProvider,
              dagRunCounters);

      List<Row> outputRows = SparkDataConverter.dagResultToRows(result);
      currentBatchOutput = outputRows.iterator();
    }
  }
}
