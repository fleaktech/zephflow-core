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

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.runner.DagResult;
import java.util.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.unsafe.types.VariantVal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for SparkDataConverter. Created by bolei on 11/1/25 */
class SparkDataConverterTest {

  private static SparkSession spark;

  @BeforeAll
  static void setupSpark() {
    spark =
        SparkSession.builder()
            .appName("SparkDataConverterTest")
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
  void testRecordToMap_simpleRecord() {
    // Create a simple RecordFleakData
    Map<String, FleakData> payload = new HashMap<>();
    payload.put("name", new StringPrimitiveFleakData("Alice"));
    payload.put("age", new NumberPrimitiveFleakData(30, NumberPrimitiveFleakData.NumberType.LONG));
    payload.put("active", new BooleanPrimitiveFleakData(true));

    RecordFleakData record = new RecordFleakData(payload);

    // Convert to Map<String, VariantVal>
    Map<String, VariantVal> result = SparkDataConverter.recordToMap(record);

    // Verify all keys present
    assertEquals(3, result.size());
    assertTrue(result.containsKey("name"));
    assertTrue(result.containsKey("age"));
    assertTrue(result.containsKey("active"));
  }

  @Test
  void testRoundTrip_primitives() {
    // Create RecordFleakData with all primitive types
    Map<String, FleakData> payload = new HashMap<>();
    payload.put("stringField", new StringPrimitiveFleakData("test"));
    payload.put(
        "intField", new NumberPrimitiveFleakData(42, NumberPrimitiveFleakData.NumberType.LONG));
    payload.put(
        "doubleField",
        new NumberPrimitiveFleakData(3.14, NumberPrimitiveFleakData.NumberType.DOUBLE));
    payload.put("boolField", new BooleanPrimitiveFleakData(false));

    RecordFleakData original = new RecordFleakData(payload);

    // Round-trip conversion
    Map<String, VariantVal> variantMap = SparkDataConverter.recordToMap(original);
    RecordFleakData reconstructed = SparkDataConverter.mapToRecord(variantMap);

    // Verify payload preserved
    assertEquals(original.getPayload().size(), reconstructed.getPayload().size());

    // Verify each field (using unwrap for comparison)
    assertEquals(
        original.getPayload().get("stringField").unwrap(),
        reconstructed.getPayload().get("stringField").unwrap());

    assertEquals(
        original.getPayload().get("boolField").unwrap(),
        reconstructed.getPayload().get("boolField").unwrap());

    // Numbers might have type differences, compare as doubles
    assertEquals(
        ((Number) original.getPayload().get("intField").unwrap()).doubleValue(),
        ((Number) reconstructed.getPayload().get("intField").unwrap()).doubleValue(),
        0.001);

    assertEquals(
        ((Number) original.getPayload().get("doubleField").unwrap()).doubleValue(),
        ((Number) reconstructed.getPayload().get("doubleField").unwrap()).doubleValue(),
        0.001);
  }

  @Test
  void testRoundTrip_nestedRecord() {
    // Create nested RecordFleakData
    Map<String, FleakData> innerPayload = new HashMap<>();
    innerPayload.put("city", new StringPrimitiveFleakData("NYC"));
    innerPayload.put("zip", new StringPrimitiveFleakData("10001"));
    RecordFleakData innerRecord = new RecordFleakData(innerPayload);

    Map<String, FleakData> outerPayload = new HashMap<>();
    outerPayload.put("name", new StringPrimitiveFleakData("Alice"));
    outerPayload.put("address", innerRecord);

    RecordFleakData original = new RecordFleakData(outerPayload);

    // Round-trip conversion
    Map<String, VariantVal> variantMap = SparkDataConverter.recordToMap(original);
    RecordFleakData reconstructed = SparkDataConverter.mapToRecord(variantMap);

    // Verify structure preserved
    assertEquals(2, reconstructed.getPayload().size());
    assertInstanceOf(RecordFleakData.class, reconstructed.getPayload().get("address"));

    RecordFleakData reconstructedInner =
        (RecordFleakData) reconstructed.getPayload().get("address");
    assertEquals("NYC", reconstructedInner.getPayload().get("city").unwrap());
    assertEquals("10001", reconstructedInner.getPayload().get("zip").unwrap());
  }

  @Test
  void testRoundTrip_array() {
    // Create RecordFleakData with array
    List<FleakData> arrayElements =
        Arrays.asList(
            new StringPrimitiveFleakData("a"),
            new StringPrimitiveFleakData("b"),
            new StringPrimitiveFleakData("c"));

    ArrayFleakData arrayField = new ArrayFleakData(arrayElements);

    Map<String, FleakData> payload = new HashMap<>();
    payload.put("items", arrayField);

    RecordFleakData original = new RecordFleakData(payload);

    // Round-trip conversion
    Map<String, VariantVal> variantMap = SparkDataConverter.recordToMap(original);
    RecordFleakData reconstructed = SparkDataConverter.mapToRecord(variantMap);

    // Verify array preserved
    assertInstanceOf(ArrayFleakData.class, reconstructed.getPayload().get("items"));
    ArrayFleakData reconstructedArray = (ArrayFleakData) reconstructed.getPayload().get("items");
    assertEquals(3, reconstructedArray.getArrayPayload().size());
  }

  @Test
  void testRoundTrip_complexNested() {
    // Create complex nested structure:
    // { users: [ { name: "Alice", scores: [1, 2, 3] }, { name: "Bob", scores: [4, 5] } ] }

    List<FleakData> aliceScores =
        Arrays.asList(
            new NumberPrimitiveFleakData(1, NumberPrimitiveFleakData.NumberType.LONG),
            new NumberPrimitiveFleakData(2, NumberPrimitiveFleakData.NumberType.LONG),
            new NumberPrimitiveFleakData(3, NumberPrimitiveFleakData.NumberType.LONG));

    Map<String, FleakData> alicePayload = new HashMap<>();
    alicePayload.put("name", new StringPrimitiveFleakData("Alice"));
    alicePayload.put("scores", new ArrayFleakData(aliceScores));
    RecordFleakData alice = new RecordFleakData(alicePayload);

    List<FleakData> bobScores =
        Arrays.asList(
            new NumberPrimitiveFleakData(4, NumberPrimitiveFleakData.NumberType.LONG),
            new NumberPrimitiveFleakData(5, NumberPrimitiveFleakData.NumberType.LONG));

    Map<String, FleakData> bobPayload = new HashMap<>();
    bobPayload.put("name", new StringPrimitiveFleakData("Bob"));
    bobPayload.put("scores", new ArrayFleakData(bobScores));
    RecordFleakData bob = new RecordFleakData(bobPayload);

    List<FleakData> users = Arrays.asList(alice, bob);

    Map<String, FleakData> rootPayload = new HashMap<>();
    rootPayload.put("users", new ArrayFleakData(users));

    RecordFleakData original = new RecordFleakData(rootPayload);

    // Round-trip conversion
    Map<String, VariantVal> variantMap = SparkDataConverter.recordToMap(original);
    RecordFleakData reconstructed = SparkDataConverter.mapToRecord(variantMap);

    // Verify structure
    ArrayFleakData usersArray = (ArrayFleakData) reconstructed.getPayload().get("users");
    assertEquals(2, usersArray.getArrayPayload().size());

    RecordFleakData firstUser = (RecordFleakData) usersArray.getArrayPayload().get(0);
    assertEquals("Alice", firstUser.getPayload().get("name").unwrap());

    ArrayFleakData firstUserScores = (ArrayFleakData) firstUser.getPayload().get("scores");
    assertEquals(3, firstUserScores.getArrayPayload().size());
  }

  @Test
  void testRoundTrip_nullValues() {
    // Create RecordFleakData with null value
    Map<String, FleakData> payload = new HashMap<>();
    payload.put("name", new StringPrimitiveFleakData("Alice"));
    payload.put("optional", null);

    RecordFleakData original = new RecordFleakData(payload);

    // Round-trip conversion
    Map<String, VariantVal> variantMap = SparkDataConverter.recordToMap(original);
    RecordFleakData reconstructed = SparkDataConverter.mapToRecord(variantMap);

    // Verify null preserved
    assertEquals(2, reconstructed.getPayload().size());
    assertNull(reconstructed.getPayload().get("optional"));
  }

  @Test
  void testRoundTrip_emptyRecord() {
    // Create empty RecordFleakData
    RecordFleakData original = new RecordFleakData(new HashMap<>());

    // Round-trip conversion
    Map<String, VariantVal> variantMap = SparkDataConverter.recordToMap(original);
    RecordFleakData reconstructed = SparkDataConverter.mapToRecord(variantMap);

    // Verify empty preserved
    assertEquals(0, reconstructed.getPayload().size());
  }

  @Test
  void testDagResultToRows_singleNode() {
    // Create DagResult with single node
    DagResult dagResult = new DagResult();

    List<RecordFleakData> events = new ArrayList<>();
    Map<String, FleakData> payload1 = new HashMap<>();
    payload1.put("id", new StringPrimitiveFleakData("1"));
    payload1.put("value", new StringPrimitiveFleakData("test1"));
    events.add(new RecordFleakData(payload1));

    Map<String, FleakData> payload2 = new HashMap<>();
    payload2.put("id", new StringPrimitiveFleakData("2"));
    payload2.put("value", new StringPrimitiveFleakData("test2"));
    events.add(new RecordFleakData(payload2));

    dagResult.getOutputEvents().put("node1", events);

    // Convert to Rows
    List<Row> rows = SparkDataConverter.dagResultToRows(dagResult);

    // Create DataFrame to attach schema
    Dataset<Row> df = spark.createDataFrame(rows, SparkSchemas.OUTPUT_EVENT_SCHEMA);
    List<Row> rowsWithSchema = df.collectAsList();

    // Verify using field names
    assertEquals(2, rowsWithSchema.size());
    assertEquals("node1", rowsWithSchema.get(0).getAs("nodeId"));
    assertEquals("node1", rowsWithSchema.get(1).getAs("nodeId"));

    // Spark returns Scala Map, convert to Java Map
    scala.collection.Map<String, VariantVal> scalaMap = rowsWithSchema.get(0).getAs("data");
    Map<String, VariantVal> data1 =
        scala.collection.JavaConverters.mapAsJavaMapConverter(scalaMap).asJava();
    assertEquals(2, data1.size());
  }

  @Test
  void testDagResultToRows_multipleNodes() {
    // Create DagResult with multiple nodes
    DagResult dagResult = new DagResult();

    Map<String, FleakData> event1 = new HashMap<>();
    event1.put("data", new StringPrimitiveFleakData("from_node1"));
    dagResult.getOutputEvents().put("node1", List.of(new RecordFleakData(event1)));

    Map<String, FleakData> event2a = new HashMap<>();
    event2a.put("data", new StringPrimitiveFleakData("from_node2_a"));
    Map<String, FleakData> event2b = new HashMap<>();
    event2b.put("data", new StringPrimitiveFleakData("from_node2_b"));
    dagResult
        .getOutputEvents()
        .put("node2", List.of(new RecordFleakData(event2a), new RecordFleakData(event2b)));

    // Convert to Rows
    List<Row> rows = SparkDataConverter.dagResultToRows(dagResult);

    // Create DataFrame to attach schema
    Dataset<Row> df = spark.createDataFrame(rows, SparkSchemas.OUTPUT_EVENT_SCHEMA);
    List<Row> rowsWithSchema = df.collectAsList();

    // Verify total count
    assertEquals(3, rowsWithSchema.size());

    // Verify node IDs using field names
    Set<String> nodeIds = new HashSet<>();
    for (Row row : rowsWithSchema) {
      nodeIds.add(row.getAs("nodeId"));
    }
    assertEquals(Set.of("node1", "node2"), nodeIds);

    // Count events per node
    long node1Count =
        rowsWithSchema.stream().filter(r -> "node1".equals(r.getAs("nodeId"))).count();
    long node2Count =
        rowsWithSchema.stream().filter(r -> "node2".equals(r.getAs("nodeId"))).count();

    assertEquals(1, node1Count);
    assertEquals(2, node2Count);
  }

  @Test
  void testDagResultToRows_emptyResult() {
    // Create empty DagResult
    DagResult dagResult = new DagResult();

    // Convert to Rows
    List<Row> rows = SparkDataConverter.dagResultToRows(dagResult);

    // Verify empty
    assertEquals(0, rows.size());
  }

  @Test
  void testInputRowToRecord() {
    // Create RecordFleakData
    Map<String, FleakData> payload = new HashMap<>();
    payload.put("field1", new StringPrimitiveFleakData("test"));
    payload.put(
        "field2", new NumberPrimitiveFleakData(123, NumberPrimitiveFleakData.NumberType.LONG));

    RecordFleakData original = new RecordFleakData(payload);

    // Convert to Map<String, VariantVal>
    Map<String, VariantVal> variantMap = SparkDataConverter.recordToMap(original);

    // Create Row with INPUT_EVENT_SCHEMA
    Row row = org.apache.spark.sql.RowFactory.create(variantMap);
    Dataset<Row> df = spark.createDataFrame(List.of(row), SparkSchemas.INPUT_EVENT_SCHEMA);
    Row rowWithSchema = df.first();

    // Convert back
    RecordFleakData reconstructed = SparkDataConverter.inputRowToRecord(rowWithSchema);

    // Verify
    assertEquals(2, reconstructed.getPayload().size());
    assertEquals("test", reconstructed.getPayload().get("field1").unwrap());
  }
}
