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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.dag.Dag;
import io.fleak.zephflow.runner.NoSourceDagRunner;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

/** Integration tests for SparkDagProcessor */
class SparkDagProcessorTest {

  private static SparkSession spark;

  @BeforeAll
  static void setupSpark() {
    spark =
        SparkSession.builder()
            .appName("SparkDagProcessorTest")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
  }

  @AfterAll
  static void teardownSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void testProcess_invalidInputSchema() {
    NoSourceDagRunner runner = createMinimalRunner();
    SparkDagProcessor processor = SparkDagProcessor.builder().dagRunner(runner).build();

    // Create dataset with wrong schema (use OUTPUT schema instead of INPUT)
    StructType wrongSchema = SparkSchemas.OUTPUT_EVENT_SCHEMA;
    Dataset<Row> wrongDataset = spark.emptyDataset(Encoders.row(wrongSchema));

    // Should throw IllegalArgumentException
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> processor.process(wrongDataset));
    assertTrue(ex.getMessage().contains("Invalid input schema"));
  }

  @Test
  void testProcess_emptyDataset() {
    NoSourceDagRunner runner = createMinimalRunner();
    SparkDagProcessor processor = SparkDagProcessor.builder().dagRunner(runner).build();

    // Create empty input dataset
    Dataset<Row> emptyDataset = spark.emptyDataset(Encoders.row(SparkSchemas.INPUT_EVENT_SCHEMA));

    // Process
    Dataset<Row> outputDataset = processor.process(emptyDataset);

    // Verify empty output
    assertEquals(0, outputDataset.count());
  }

  @Test
  void testProcess_outputSchema() {
    NoSourceDagRunner runner = createMinimalRunner();
    SparkDagProcessor processor = SparkDagProcessor.builder().dagRunner(runner).build();

    // Create input dataset
    Dataset<Row> inputDataset = createInputDataset(5);

    // Process
    Dataset<Row> outputDataset = processor.process(inputDataset);

    // Verify output schema matches expected
    assertEquals(SparkSchemas.OUTPUT_EVENT_SCHEMA, outputDataset.schema());
  }

  @Test
  void testConfig_defaultValues() {
    SparkDagProcessor.Config config = SparkDagProcessor.Config.builder().build();
    assertEquals(1000, config.getBatchSize());
  }

  @Test
  void testConfig_customBatchSize() {
    SparkDagProcessor.Config config = SparkDagProcessor.Config.builder().batchSize(500).build();
    assertEquals(500, config.getBatchSize());
  }

  @Test
  void testBuilder_withConfig() {
    NoSourceDagRunner runner = createMinimalRunner();
    SparkDagProcessor.Config config = SparkDagProcessor.Config.builder().batchSize(100).build();

    SparkDagProcessor processor =
        SparkDagProcessor.builder().dagRunner(runner).config(config).build();

    // Process to verify it works
    Dataset<Row> inputDataset = createInputDataset(10);
    Dataset<Row> outputDataset = processor.process(inputDataset);

    assertEquals(SparkSchemas.OUTPUT_EVENT_SCHEMA, outputDataset.schema());
  }

  @Test
  void testProcess_multiplePartitions() {
    NoSourceDagRunner runner = createMinimalRunner();
    SparkDagProcessor processor =
        SparkDagProcessor.builder()
            .dagRunner(runner)
            .config(SparkDagProcessor.Config.builder().batchSize(5).build())
            .build();

    // Create input dataset with multiple partitions
    Dataset<Row> inputDataset = createInputDataset(20).repartition(4);

    // Verify partitions
    assertEquals(4, inputDataset.rdd().getNumPartitions());

    // Process
    Dataset<Row> outputDataset = processor.process(inputDataset);

    // Verify output schema
    assertEquals(SparkSchemas.OUTPUT_EVENT_SCHEMA, outputDataset.schema());
  }

  @Test
  void testProcess_verifiesOutputData() throws Exception {
    NoSourceDagRunner runner = createRealDagRunner();
    SparkDagProcessor processor = SparkDagProcessor.builder().dagRunner(runner).build();

    // Create 10 input events (0-9) in single partition
    Dataset<Row> inputDataset = createInputDataset(10).coalesce(1);

    // Process
    Dataset<Row> outputDataset = processor.process(inputDataset);
    List<Row> results = outputDataset.collectAsList();

    // Verify: 2 outputs (one summary per sink: c and d)
    assertEquals(2, results.size());

    // Verify we have outputs from both sinks
    Set<String> nodeIds = results.stream().map(r -> r.getString(0)).collect(Collectors.toSet());
    assertEquals(Set.of("c", "d"), nodeIds);

    // Verify data content: check sink summaries
    results.forEach(
        row -> {
          // Get Scala Map from Row, then convert to Java Map
          scala.collection.Map<String, VariantVal> scalaMap = row.getAs(1);
          Map<String, VariantVal> dataMap = JavaConverters.mapAsJavaMapConverter(scalaMap).asJava();

          // Each sink should have received 5 events (evens or odds)
          long inputCount =
              Long.parseLong(
                  new Variant(
                          dataMap.get("inputCount").getValue(),
                          dataMap.get("inputCount").getMetadata())
                      .toJson(ZoneOffset.UTC));
          long successCount =
              Long.parseLong(
                  new Variant(
                          dataMap.get("successCount").getValue(),
                          dataMap.get("successCount").getMetadata())
                      .toJson(ZoneOffset.UTC));
          assertEquals(5L, inputCount);
          assertEquals(5L, successCount);
        });
  }

  @Test
  void testProcess_batchBoundary_exactMatch() throws Exception {
    NoSourceDagRunner runner = createRealDagRunner();
    SparkDagProcessor processor =
        SparkDagProcessor.builder()
            .dagRunner(runner)
            .config(SparkDagProcessor.Config.builder().batchSize(10).build())
            .build();

    // Exactly 10 events (1 batch) in single partition
    Dataset<Row> inputDataset = createInputDataset(10).coalesce(1);
    Dataset<Row> outputDataset = processor.process(inputDataset);

    // Verify: 2 outputs (one summary per sink: c and d)
    assertEquals(2, outputDataset.count());

    // Verify we have outputs from both sinks
    List<Row> results = outputDataset.collectAsList();
    Set<String> nodeIds = results.stream().map(r -> r.getString(0)).collect(Collectors.toSet());
    assertEquals(Set.of("c", "d"), nodeIds);
  }

  @Test
  void testProcess_batchBoundary_partialBatch() throws Exception {
    NoSourceDagRunner runner = createRealDagRunner();
    SparkDagProcessor processor =
        SparkDagProcessor.builder()
            .dagRunner(runner)
            .config(SparkDagProcessor.Config.builder().batchSize(5).build())
            .build();

    // 11 events → 2 full batches (5+5) + 1 partial (1) in single partition
    Dataset<Row> inputDataset = createInputDataset(11).coalesce(1);
    Dataset<Row> outputDataset = processor.process(inputDataset);

    // Verify: 6 outputs (3 batches × 2 sinks)
    assertEquals(6, outputDataset.count());

    // Verify we have outputs from both sinks
    // Data: 6 evens (0,2,4,6,8,10) → sink c, 5 odds (1,3,5,7,9) → sink d
    List<Row> results = outputDataset.collectAsList();
    Set<String> nodeIds = results.stream().map(r -> r.getString(0)).collect(Collectors.toSet());
    assertEquals(Set.of("c", "d"), nodeIds);
  }

  // Helper methods

  private NoSourceDagRunner createMinimalRunner() {
    return new NoSourceDagRunner(
        Collections.emptyList(),
        new Dag<>(Collections.emptyList(), Collections.emptyList()),
        JobContext.builder().build());
  }

  private NoSourceDagRunner createRealDagRunner() throws Exception {
    return TestDagRunnerUtil.createRealDagRunner();
  }

  private Dataset<Row> createInputDataset(int count) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("num", i));
      Map<String, VariantVal> variantMap = SparkDataConverter.recordToMap(record);
      Row row = RowFactory.create(variantMap);
      rows.add(row);
    }

    return spark.createDataFrame(rows, SparkSchemas.INPUT_EVENT_SCHEMA);
  }
}
