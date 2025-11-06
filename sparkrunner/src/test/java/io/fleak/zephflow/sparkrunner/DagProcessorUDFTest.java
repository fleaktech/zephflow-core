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
import java.io.*;
import java.time.ZoneOffset;
import java.util.*;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

/** Tests for DagProcessorUDF */
class DagProcessorUDFTest {

  private static SparkSession spark;

  @BeforeAll
  static void setupSpark() {
    spark =
        SparkSession.builder()
            .appName("DagProcessorUDFTest")
            .master("local[1]")
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
  void testBatchSizeValidation_positive() {
    NoSourceDagRunner runner = createMinimalRunner();
    assertDoesNotThrow(() -> new DagProcessorUDF(runner, 1000));
  }

  @Test
  void testBatchSizeValidation_zero() {
    NoSourceDagRunner runner = createMinimalRunner();
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> new DagProcessorUDF(runner, 0));
    assertTrue(ex.getMessage().contains("batchSize must be positive"));
  }

  @Test
  void testBatchSizeValidation_negative() {
    NoSourceDagRunner runner = createMinimalRunner();
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> new DagProcessorUDF(runner, -5));
    assertTrue(ex.getMessage().contains("batchSize must be positive"));
  }

  @Test
  void testSerialization() throws Exception {
    NoSourceDagRunner runner = createMinimalRunner();
    DagProcessorUDF original = new DagProcessorUDF(runner, 1000);

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(original);
    }

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DagProcessorUDF deserialized;
    try (ObjectInputStream ois = new ObjectInputStream(bais)) {
      deserialized = (DagProcessorUDF) ois.readObject();
    }

    // Verify fields
    assertEquals(original.batchSize(), deserialized.batchSize());
    assertNotNull(deserialized.noSourceDagRunner());
  }

  @Test
  void testProcessEmptyPartition() {
    NoSourceDagRunner runner = createMinimalRunner();
    DagProcessorUDF udf = new DagProcessorUDF(runner, 10);

    // Empty input
    List<Row> inputRows = new ArrayList<>();

    // Process
    @SuppressWarnings("RedundantOperationOnEmptyContainer")
    Iterator<Row> result = udf.call(inputRows.iterator());

    // Verify no output
    assertFalse(result.hasNext());
  }

  @Test
  void testProcessSingleBatch() throws Exception {
    NoSourceDagRunner runner = createRealDagRunner();
    DagProcessorUDF udf = new DagProcessorUDF(runner, 10); // batchSize=10

    // Create 5 input events: {num: 0}, {num: 1}, ..., {num: 4}
    List<Row> inputRows = createInputRowsWithSchema(5);

    // Process
    Iterator<Row> result = udf.call(inputRows.iterator());
    List<Row> outputRows = collectAll(result);

    // Verify: 2 outputs (one summary per sink: c and d)
    assertEquals(2, outputRows.size());

    // Verify we have outputs from both sinks
    Set<String> nodeIds = outputRows.stream().map(r -> r.getString(0)).collect(Collectors.toSet());
    assertEquals(Set.of("c", "d"), nodeIds);

    // Verify sink summaries have inputCount and successCount
    outputRows.forEach(
        row -> {
          // Get Scala Map from Row, then convert to Java Map
          scala.collection.Map<String, VariantVal> scalaMap = row.getAs(1);
          Map<String, VariantVal> dataMap = JavaConverters.mapAsJavaMapConverter(scalaMap).asJava();
          assertTrue(dataMap.containsKey("inputCount"));
          assertTrue(dataMap.containsKey("successCount"));
        });
  }

  @Test
  void testProcessMultipleBatches() throws Exception {
    NoSourceDagRunner runner = createRealDagRunner();
    DagProcessorUDF udf = new DagProcessorUDF(runner, 3); // Small batches

    // Create 10 events (0-9) → 4 batches (3+3+3+1)
    List<Row> inputRows = createInputRowsWithSchema(10);

    Iterator<Row> result = udf.call(inputRows.iterator());
    List<Row> outputRows = collectAll(result);

    // Verify: 8 outputs (2 summaries per batch × 4 batches = 8)
    assertEquals(8, outputRows.size());

    // Verify we have outputs from both sinks
    Set<String> nodeIds = outputRows.stream().map(r -> r.getString(0)).collect(Collectors.toSet());
    assertEquals(Set.of("c", "d"), nodeIds);

    // Verify total counts across all batches
    // Even numbers: 0,2,4,6,8 → 5 events total to sink c
    // Odd numbers: 1,3,5,7,9 → 5 events total to sink d
    long totalEvenInput =
        outputRows.stream()
            .filter(r -> "c".equals(r.getString(0)))
            .mapToLong(
                r -> {
                  // Get Scala Map from Row, then convert to Java Map
                  scala.collection.Map<String, VariantVal> scalaMap = r.getAs(1);
                  Map<String, VariantVal> dataMap =
                      JavaConverters.mapAsJavaMapConverter(scalaMap).asJava();
                  return extractNumFromVariant(dataMap.get("inputCount"));
                })
            .sum();
    long totalOddInput =
        outputRows.stream()
            .filter(r -> "d".equals(r.getString(0)))
            .mapToLong(
                r -> {
                  // Get Scala Map from Row, then convert to Java Map
                  scala.collection.Map<String, VariantVal> scalaMap = r.getAs(1);
                  Map<String, VariantVal> dataMap =
                      JavaConverters.mapAsJavaMapConverter(scalaMap).asJava();
                  return extractNumFromVariant(dataMap.get("inputCount"));
                })
            .sum();
    assertEquals(5L, totalEvenInput);
    assertEquals(5L, totalOddInput);
  }

  @Test
  void testStreamingBehavior_lazyEvaluation() throws Exception {
    NoSourceDagRunner runner = createRealDagRunner();
    DagProcessorUDF udf = new DagProcessorUDF(runner, 3);

    List<Row> inputRows = createInputRowsWithSchema(10);
    Iterator<Row> result = udf.call(inputRows.iterator());

    // Verify hasNext() works without consuming
    assertTrue(result.hasNext());
    //noinspection ConstantExpression,ConstantValue
    assertTrue(result.hasNext()); // Can call multiple times

    // Consume first element
    Row first = result.next();
    assertNotNull(first);

    // Verify more available
    assertTrue(result.hasNext());

    // Consume rest
    int count = 1; // Already got first
    while (result.hasNext()) {
      result.next();
      count++;
    }
    // Should get 8 output rows (2 per batch × 4 batches)
    assertEquals(8, count);

    // Verify iterator exhausted
    assertFalse(result.hasNext());
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

  private List<Row> createInputRowsWithSchema(int count) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("num", i));
      Map<String, VariantVal> variantMap = SparkDataConverter.recordToMap(record);
      rows.add(RowFactory.create(variantMap));
    }
    return spark.createDataFrame(rows, SparkSchemas.INPUT_EVENT_SCHEMA).collectAsList();
  }

  private List<Row> collectAll(Iterator<Row> iter) {
    List<Row> result = new ArrayList<>();
    iter.forEachRemaining(result::add);
    return result;
  }

  private long extractNumFromVariant(VariantVal v) {
    Variant variant = new Variant(v.getValue(), v.getMetadata());
    String json = variant.toJson(ZoneOffset.UTC);
    return Long.parseLong(json);
  }
}
