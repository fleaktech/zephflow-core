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
package io.fleak.zephflow.lib.commands.deltalakesink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.delta.kernel.types.*;
import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeSinkDto.Config;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

class DeltaLakeSinkCommandIntegrationTest {

  @Test
  void testDeltaLakeSinkFactory() {
    // Test that we can create the Delta Lake sink command using the factory
    JobContext jobContext = Mockito.mock(JobContext.class);

    DeltaLakeSinkCommandFactory factory = new DeltaLakeSinkCommandFactory();
    OperatorCommand sinkCommand = factory.createCommand("test-node", jobContext);

    // Verify the command was created
    assertNotNull(sinkCommand);
    assertInstanceOf(DeltaLakeSinkCommand.class, sinkCommand);

    // Verify the command type
    assertEquals(CommandType.SINK, factory.commandType());
  }

  @Test
  void testDeltaLakeWriterIntegration(@TempDir Path tempDir) throws Exception {
    // Test the writer component directly (which is what does the actual work)
    Config config =
        Config.builder().tablePath(tempDir.resolve("integration-test-table").toString()).build();

    // Create writer with mock JobContext
    JobContext mockJobContext = mock(JobContext.class);
    DeltaLakeWriter writer = new DeltaLakeWriter(config, mockJobContext);
    writer.initialize();

    // Create test data
    Map<String, Object> testData =
        Map.of("id", 1, "name", "John Doe", "department", "Engineering", "salary", 100000);

    // Create mock RecordFleakData for error handling
    RecordFleakData mockEvent = mock(RecordFleakData.class);
    when(mockEvent.unwrap()).thenReturn(testData);

    // Create mock prepared input events
    @SuppressWarnings("unchecked")
    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> preparedEvents =
        mock(SimpleSinkCommand.PreparedInputEvents.class);
    when(preparedEvents.preparedList()).thenReturn(List.of(testData));
    when(preparedEvents.rawAndPreparedList()).thenReturn(List.of(Pair.of(mockEvent, testData)));

    // Test flush operation - should fail because table doesn't exist
    SimpleSinkCommand.FlushResult result = writer.flush(preparedEvents, Map.of());

    // Verify error behavior - table doesn't exist so write should fail
    assertNotNull(result);
    assertEquals(0, result.successCount()); // No successful writes
    assertEquals(0, result.flushedDataSize()); // No data written
    assertFalse(result.errorOutputList().isEmpty()); // Should have errors

    // Clean up
    writer.close();
  }

  @Test
  void testMessageProcessor() {
    // Test the message processor component with simplified direct unwrap approach
    DeltaLakeMessageProcessor processor = new DeltaLakeMessageProcessor();

    // Create test event
    RecordFleakData event = mock(RecordFleakData.class);
    Map<String, Object> nestedData =
        Map.of(
            "user", Map.of("id", 123, "name", "John"),
            "metadata", Map.of("timestamp", System.currentTimeMillis()));
    when(event.unwrap()).thenReturn(nestedData);

    // Process the event
    Map<String, Object> processed = processor.preprocess(event, System.currentTimeMillis());

    // Verify direct unwrap preserves structure (no more flattening)
    assertNotNull(processed);
    assertTrue(processed.containsKey("_fleak_timestamp"));
    assertTrue(processed.containsKey("user"));
    assertTrue(processed.containsKey("metadata"));

    // Verify nested structures are preserved
    @SuppressWarnings("unchecked")
    Map<String, Object> user = (Map<String, Object>) processed.get("user");
    assertEquals(123, user.get("id"));
    assertEquals("John", user.get("name"));
  }

  @Test
  void testFleakDataTypeInference() {
    // Test that DeltaLakeDataConverter properly infers FleakData types

    // Test primitive FleakData types
    assertEquals(
        3,
        DeltaLakeDataConverter.inferSchema(
                List.of(
                    Map.of(
                        "stringField",
                        new StringPrimitiveFleakData("test"),
                        "numberField",
                        new NumberPrimitiveFleakData(
                            42.0, NumberPrimitiveFleakData.NumberType.LONG),
                        "booleanField",
                        new BooleanPrimitiveFleakData(true))))
            .fields()
            .size());
  }

  @Test
  void testComplexFleakDataStructures() {
    // Create complex FleakData structure with nested records and arrays
    Map<String, FleakData> userPayload =
        Map.of(
            "id", new NumberPrimitiveFleakData(123.0, NumberPrimitiveFleakData.NumberType.LONG),
            "name", new StringPrimitiveFleakData("John Doe"),
            "active", new BooleanPrimitiveFleakData(true));
    RecordFleakData userRecord = new RecordFleakData(userPayload);

    List<FleakData> tags =
        List.of(new StringPrimitiveFleakData("engineer"), new StringPrimitiveFleakData("senior"));
    ArrayFleakData tagsArray = new ArrayFleakData(tags);

    Map<String, FleakData> rootPayload =
        Map.of(
            "user", userRecord,
            "tags", tagsArray,
            "timestamp",
                new NumberPrimitiveFleakData(
                    System.currentTimeMillis(), NumberPrimitiveFleakData.NumberType.LONG));
    RecordFleakData complexEvent = new RecordFleakData(rootPayload);

    // Test message processor with complex FleakData
    DeltaLakeMessageProcessor processor = new DeltaLakeMessageProcessor();
    Map<String, Object> processed = processor.preprocess(complexEvent, System.currentTimeMillis());

    // Verify structure preservation
    assertNotNull(processed);
    assertTrue(processed.containsKey("user"));
    assertTrue(processed.containsKey("tags"));
    assertTrue(processed.containsKey("timestamp"));
    assertTrue(processed.containsKey("_fleak_timestamp"));

    // Verify nested record unwrapping
    @SuppressWarnings("unchecked")
    Map<String, Object> unwrappedUser = (Map<String, Object>) processed.get("user");
    assertEquals(
        123.0,
        ((Number) unwrappedUser.get("id"))
            .doubleValue()); // NumberPrimitiveFleakData stores as double
    assertEquals("John Doe", unwrappedUser.get("name"));
    assertEquals(true, unwrappedUser.get("active"));

    // Verify array unwrapping
    @SuppressWarnings("unchecked")
    List<Object> unwrappedTags = (List<Object>) processed.get("tags");
    assertEquals("engineer", unwrappedTags.get(0));
    assertEquals("senior", unwrappedTags.get(1));
  }

  @Test
  void testDataConverterWithFleakData() {
    // Test DeltaLakeDataConverter with actual FleakData objects
    Map<String, Object> testData =
        Map.of(
            "stringField", new StringPrimitiveFleakData("test"),
            "longField",
                new NumberPrimitiveFleakData(42.0, NumberPrimitiveFleakData.NumberType.LONG),
            "doubleField",
                new NumberPrimitiveFleakData(3.14, NumberPrimitiveFleakData.NumberType.DOUBLE),
            "boolField", new BooleanPrimitiveFleakData(true),
            "arrayField",
                new ArrayFleakData(
                    List.of(
                        new StringPrimitiveFleakData("item1"),
                        new StringPrimitiveFleakData("item2"))),
            "recordField",
                new RecordFleakData(
                    Map.of(
                        "nestedString", new StringPrimitiveFleakData("nested"),
                        "nestedNumber",
                            new NumberPrimitiveFleakData(
                                99.0, NumberPrimitiveFleakData.NumberType.LONG))));

    // Test schema inference
    var schema = DeltaLakeDataConverter.inferSchema(List.of(testData));
    assertNotNull(schema);
    assertEquals(6, schema.fields().size());

    // Verify correct type inference for each field
    var fieldMap = new HashMap<String, DataType>();
    for (var field : schema.fields()) {
      fieldMap.put(field.getName(), field.getDataType());
    }

    assertEquals(StringType.STRING, fieldMap.get("stringField"));
    assertEquals(LongType.LONG, fieldMap.get("longField"));
    assertEquals(DoubleType.DOUBLE, fieldMap.get("doubleField"));
    assertEquals(BooleanType.BOOLEAN, fieldMap.get("boolField"));
    assertInstanceOf(ArrayType.class, fieldMap.get("arrayField"));
    assertInstanceOf(StructType.class, fieldMap.get("recordField"));
  }

  @Test
  void testNonNullableFieldValidation() {
    // Test that writing NULL to a non-nullable field throws an appropriate error
    // This simulates the INVOICE_ID vs INVOICE_I scenario

    // Create a schema with a non-nullable field (like INVOICE_ID in the ATTACHMENT table)
    List<StructField> fields =
        List.of(
            new StructField("ATTACHMENT_ID", StringType.STRING, true), // nullable
            new StructField("INVOICE_ID", StringType.STRING, false), // non-nullable (required)
            new StructField("DESCRIPTION", StringType.STRING, true) // nullable
            );
    StructType schema = new StructType(fields);

    // Create input data that's missing the required INVOICE_ID field
    // (simulating the case where input has INVOICE_I instead of INVOICE_ID)
    Map<String, Object> recordMissingRequiredField = new HashMap<>();
    recordMissingRequiredField.put("ATTACHMENT_ID", "ATT-123");
    recordMissingRequiredField.put("INVOICE_I", "INV-456"); // Wrong field name!
    recordMissingRequiredField.put("DESCRIPTION", "Test attachment");

    List<Map<String, Object>> data = List.of(recordMissingRequiredField);

    // Attempt to convert - should throw exception
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    // Verify the error message is helpful
    String errorMessage = exception.getMessage();
    assertTrue(errorMessage.contains("Cannot write NULL to non-nullable field 'INVOICE_ID'"));
    assertTrue(errorMessage.contains("Available fields in input:"));
    assertTrue(errorMessage.contains("INVOICE_I")); // Shows the actual field that exists
    assertTrue(errorMessage.contains("ATTACHMENT_ID"));
    assertTrue(errorMessage.contains("DESCRIPTION"));
  }

  @Test
  void testNullableFieldAllowsNull() {
    // Test that NULL values are allowed for nullable fields
    List<StructField> fields =
        List.of(
            new StructField("ID", StringType.STRING, false), // non-nullable
            new StructField("OPTIONAL_FIELD", StringType.STRING, true) // nullable
            );
    StructType schema = new StructType(fields);

    Map<String, Object> record = new HashMap<>();
    record.put("ID", "123");
    record.put("OPTIONAL_FIELD", null); // NULL is OK for nullable field

    List<Map<String, Object>> data = List.of(record);

    // This should succeed without throwing exception
    var result = DeltaLakeDataConverter.convertToColumnarBatch(data, schema);
    assertNotNull(result);

    // Verify the iterator has data
    assertTrue(result.hasNext());
    var batch = result.next();
    assertNotNull(batch);

    // Close the iterator
    try {
      result.close();
    } catch (Exception e) {
      // Ignore close errors in test
    }
  }
}
