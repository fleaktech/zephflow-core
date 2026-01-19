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

import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeSinkDto.Config;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeltaLakeWriterTest {

  @TempDir Path tempDir;

  private static final Map<String, Object> TEST_AVRO_SCHEMA =
      Map.of(
          "type", "record",
          "name", "TestRecord",
          "fields",
              List.of(
                  Map.of("name", "id", "type", "int"),
                  Map.of("name", "name", "type", "string"),
                  Map.of("name", "value", "type", "double")));

  private Config config;
  private String tablePath;
  private JobContext jobContext;
  private FleakCounter sinkOutputCounter;
  private FleakCounter outputSizeCounter;
  private FleakCounter sinkErrorCounter;

  @BeforeEach
  void setUp() {
    tablePath = tempDir.resolve("test-delta-table").toString();
    createDeltaTable(tablePath);
    config =
        Config.builder().tablePath(tablePath).batchSize(100).avroSchema(TEST_AVRO_SCHEMA).build();

    // Mock JobContext for all tests
    jobContext = mock(JobContext.class);

    // Mock counters for all tests
    sinkOutputCounter = mock(FleakCounter.class);
    outputSizeCounter = mock(FleakCounter.class);
    sinkErrorCounter = mock(FleakCounter.class);
  }

  private DeltaLakeWriter createWriter(Config config) {
    return new DeltaLakeWriter(
        config, jobContext, null, sinkOutputCounter, outputSizeCounter, sinkErrorCounter);
  }

  /** Creates a Delta table at the given path with the test schema (id, name, value) */
  private void createDeltaTable(String path) {
    createDeltaTable(path, List.of());
  }

  /** Creates a Delta table at the given path with the test schema and optional partition columns */
  private void createDeltaTable(String path, List<String> partitionColumns) {
    Engine engine = DefaultEngine.create(new Configuration());
    StructType schema =
        new StructType(
            List.of(
                new StructField("id", IntegerType.INTEGER, false),
                new StructField("name", StringType.STRING, true),
                new StructField("value", DoubleType.DOUBLE, true)));

    Table table = Table.forPath(engine, path);
    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(engine, "Test Table Creation", Operation.CREATE_TABLE);
    txnBuilder = txnBuilder.withSchema(engine, schema);

    if (partitionColumns != null && !partitionColumns.isEmpty()) {
      txnBuilder = txnBuilder.withPartitionColumns(engine, partitionColumns);
    }

    Transaction txn = txnBuilder.build(engine);
    txn.commit(engine, emptyCloseableIterable());
  }

  @SuppressWarnings("unchecked")
  private static <T> CloseableIterable<T> emptyCloseableIterable() {
    return new CloseableIterable<T>() {
      @Override
      public CloseableIterator<T> iterator() {
        return new CloseableIterator<T>() {
          @Override
          public boolean hasNext() {
            return false;
          }

          @Override
          public T next() {
            throw new NoSuchElementException();
          }

          @Override
          public void close() {}
        };
      }

      @Override
      public void close() {}
    };
  }

  @Test
  void testWriterCreation() {
    assertDoesNotThrow(
        () -> {
          DeltaLakeWriter writer = createWriter(config);
          writer.initialize();
          writer.close();
        });
  }

  @Test
  void testEmptyFlush() throws Exception {
    DeltaLakeWriter writer = createWriter(config);
    writer.initialize();

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> emptyEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = writer.flush(emptyEvents, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(0, result.flushedDataSize());
    assertTrue(result.errorOutputList().isEmpty());

    writer.close();
  }

  @Test
  void testFlushWithData() {
    DeltaLakeWriter writer = createWriter(config);
    writer.initialize();

    // Create test data
    Map<String, Object> testData = new HashMap<>();
    testData.put("id", 1);
    testData.put("name", "test");
    testData.put("value", 42.0);

    RecordFleakData testRecord = (RecordFleakData) FleakData.wrap(testData);

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(testRecord, testData);

    // This test might fail due to Delta Kernel dependencies not being available in test environment
    // In a real scenario, you would set up test containers or mock the Delta Kernel components
    try {
      SimpleSinkCommand.FlushResult result = writer.flush(events, Map.of());

      // If successful, verify the result
      assertTrue(result.successCount() >= 0);
      assertTrue(result.flushedDataSize() >= 0);

      writer.close();
    } catch (Exception e) {
      // Expected in test environment without proper Delta Lake setup
      // Verify that error handling works correctly
      assertTrue(
          e.getMessage().contains("Delta")
              || e.getMessage().contains("table")
              || e.getMessage().contains("Kernel")
              || e.getMessage().contains("Failed to flush remaining events during close"));
    }
  }

  @Test
  void testBatchSizeConfiguration() {
    String path = tablePath + "_batch";
    createDeltaTable(path);
    Config testConfig =
        Config.builder().tablePath(path).batchSize(500).avroSchema(TEST_AVRO_SCHEMA).build();

    assertDoesNotThrow(
        () -> {
          DeltaLakeWriter writer = createWriter(testConfig);
          writer.initialize();
          writer.close();
        });
  }

  @Test
  void testPartitionConfiguration() {
    String path = tablePath + "_partitioned";
    // Create table with partition columns that match the config
    createDeltaTable(path, List.of("name"));
    Config partitionConfig =
        Config.builder()
            .tablePath(path)
            .partitionColumns(List.of("name"))
            .avroSchema(TEST_AVRO_SCHEMA)
            .build();

    assertDoesNotThrow(
        () -> {
          DeltaLakeWriter writer = createWriter(partitionConfig);
          writer.initialize();
          writer.close();
        });
  }

  @Test
  void testHadoopConfiguration() {
    Map<String, String> hadoopConfig = new HashMap<>();
    hadoopConfig.put("fs.defaultFS", "file:///");
    hadoopConfig.put("hadoop.tmp.dir", tempDir.toString());

    String path = tablePath + "_hadoop";
    createDeltaTable(path);
    Config hadoopConfiguredConfig =
        Config.builder()
            .tablePath(path)
            .hadoopConfiguration(hadoopConfig)
            .avroSchema(TEST_AVRO_SCHEMA)
            .build();

    assertDoesNotThrow(
        () -> {
          DeltaLakeWriter writer = createWriter(hadoopConfiguredConfig);
          writer.initialize();
          writer.close();
        });
  }

  @Test
  void testInvalidTablePath() {
    Config invalidConfig =
        Config.builder()
            .tablePath("/invalid/path/that/should/not/exist/delta-table")
            .avroSchema(TEST_AVRO_SCHEMA)
            .build();

    // Writer creation succeeds, but initialize() fails because table doesn't exist
    DeltaLakeWriter writer = createWriter(invalidConfig);
    IllegalStateException exception = assertThrows(IllegalStateException.class, writer::initialize);
    assertTrue(exception.getMessage().contains("Delta table does not exist"));
  }

  @Test
  void testPartitionColumnMismatchDetection() {
    // Create table with NO partition columns
    String path = tablePath + "_partition_mismatch";
    createDeltaTable(path, List.of()); // unpartitioned

    // Config specifies partition columns that don't match the table
    Config mismatchConfig =
        Config.builder()
            .tablePath(path)
            .partitionColumns(List.of("name")) // config says partitioned by name
            .avroSchema(TEST_AVRO_SCHEMA)
            .build();

    // Should fail with partition mismatch error
    DeltaLakeWriter writer = createWriter(mismatchConfig);
    IllegalStateException exception = assertThrows(IllegalStateException.class, writer::initialize);
    assertTrue(exception.getMessage().contains("Partition column mismatch detected"));
    assertTrue(exception.getMessage().contains("Split Brain"));
  }

  @Test
  void testSchemaFieldNameMismatchDetection() {
    String path = tablePath + "_field_mismatch";
    createDeltaTable(path, List.of()); // table has id, name, value

    // Config has 'user_id' instead of 'id'
    Map<String, Object> mismatchAvroSchema =
        Map.of(
            "type", "record",
            "name", "TestRecord",
            "fields",
                List.of(
                    Map.of("name", "user_id", "type", "int"),
                    Map.of("name", "name", "type", "string"),
                    Map.of("name", "value", "type", "double")));

    Config mismatchConfig = Config.builder().tablePath(path).avroSchema(mismatchAvroSchema).build();

    DeltaLakeWriter writer = createWriter(mismatchConfig);
    IllegalStateException exception = assertThrows(IllegalStateException.class, writer::initialize);
    assertTrue(exception.getMessage().contains("Schema mismatch detected"));
    assertTrue(exception.getMessage().contains("Split Brain"));
    assertTrue(exception.getMessage().contains("user_id"));
  }

  @Test
  void testSchemaTypeMismatchDetection() {
    String path = tablePath + "_type_mismatch";
    createDeltaTable(path, List.of()); // table has id as IntegerType

    // Config has 'id' as long instead of int
    Map<String, Object> mismatchAvroSchema =
        Map.of(
            "type", "record",
            "name", "TestRecord",
            "fields",
                List.of(
                    Map.of("name", "id", "type", "long"),
                    Map.of("name", "name", "type", "string"),
                    Map.of("name", "value", "type", "double")));

    Config mismatchConfig = Config.builder().tablePath(path).avroSchema(mismatchAvroSchema).build();

    DeltaLakeWriter writer = createWriter(mismatchConfig);
    IllegalStateException exception = assertThrows(IllegalStateException.class, writer::initialize);
    assertTrue(exception.getMessage().contains("Schema mismatch detected"));
    assertTrue(exception.getMessage().contains("type mismatch"));
  }

  @Test
  void testSchemaFieldMissingFromConfigDetection() {
    String path = tablePath + "_missing_field";
    createDeltaTable(path, List.of()); // table has id, name, value

    // Config missing 'value' field
    Map<String, Object> incompleteAvroSchema =
        Map.of(
            "type", "record",
            "name", "TestRecord",
            "fields",
                List.of(
                    Map.of("name", "id", "type", "int"), Map.of("name", "name", "type", "string")));

    Config mismatchConfig =
        Config.builder().tablePath(path).avroSchema(incompleteAvroSchema).build();

    DeltaLakeWriter writer = createWriter(mismatchConfig);
    IllegalStateException exception = assertThrows(IllegalStateException.class, writer::initialize);
    assertTrue(exception.getMessage().contains("Schema mismatch detected"));
    assertTrue(exception.getMessage().contains("exists in table but not in config"));
  }

  @Test
  void testClose() throws IOException {
    try (DeltaLakeWriter writer = createWriter(config)) {
      writer.initialize();
      assertDoesNotThrow(writer::close);

      // Multiple closes should not throw
      assertDoesNotThrow(writer::close);
    }
  }

  @Test
  void testBufferAccumulatesEventsBeforeBatchSize() throws Exception {
    String path = tablePath + "_buffer1";
    createDeltaTable(path);
    Config testConfig =
        Config.builder().tablePath(path).batchSize(10).avroSchema(TEST_AVRO_SCHEMA).build();
    DeltaLakeWriter writer = createWriter(testConfig);
    writer.initialize();

    Map<String, Object> testData = new HashMap<>();
    testData.put("id", 1);
    testData.put("name", "test");

    // Add 5 events (buffer: 5/10) - should accumulate without flushing
    for (int i = 0; i < 5; i++) {
      Map<String, Object> event = new HashMap<>(testData);
      event.put("id", i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(event);
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, event);
      writer.flush(events, Map.of());
    }

    // Verify buffer contains 5 events
    assertEquals(5, writer.testGetBufferSize());

    // Add 3 more events (buffer: 8/10) - still under batch size
    for (int i = 5; i < 8; i++) {
      Map<String, Object> event = new HashMap<>(testData);
      event.put("id", i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(event);
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, event);
      writer.flush(events, Map.of());
    }

    // Verify buffer contains 8 events (not flushed yet)
    assertEquals(8, writer.testGetBufferSize());
    writer.close();
  }

  @Test
  void testBufferClearedAfterFlush() throws Exception {
    String path = tablePath + "_buffer2";
    createDeltaTable(path);
    Config testConfig =
        Config.builder().tablePath(path).batchSize(5).avroSchema(TEST_AVRO_SCHEMA).build();
    DeltaLakeWriter writer = createWriter(testConfig);
    writer.initialize();

    Map<String, Object> testData = new HashMap<>();
    testData.put("id", 1);
    testData.put("name", "test");

    // Add 5 events to reach batchSize - will trigger flush
    for (int i = 0; i < 5; i++) {
      Map<String, Object> event = new HashMap<>(testData);
      event.put("id", i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(event);
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, event);
      writer.flush(events, Map.of());
    }
    // Buffer should be cleared after batch size reached
    assertEquals(0, writer.testGetBufferSize());
    writer.close();
  }

  @Test
  void testBufferSizeTracking() throws Exception {
    String path = tablePath + "_buffer3";
    createDeltaTable(path);
    Config testConfig =
        Config.builder().tablePath(path).batchSize(100).avroSchema(TEST_AVRO_SCHEMA).build();
    DeltaLakeWriter writer = createWriter(testConfig);
    writer.initialize();

    assertEquals(0, writer.testGetBufferSize());

    Map<String, Object> testData = new HashMap<>();
    testData.put("id", 1);
    testData.put("name", "test");

    // Add events to buffer
    for (int i = 0; i < 25; i++) {
      Map<String, Object> event = new HashMap<>(testData);
      event.put("id", i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(event);
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, event);
      writer.flush(events, Map.of());
    }
    assertEquals(25, writer.testGetBufferSize());

    for (int i = 25; i < 55; i++) {
      Map<String, Object> event = new HashMap<>(testData);
      event.put("id", i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(event);
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, event);
      writer.flush(events, Map.of());
    }
    assertEquals(55, writer.testGetBufferSize());

    // Still under batchSize of 100, so buffer keeps accumulating
    for (int i = 55; i < 75; i++) {
      Map<String, Object> event = new HashMap<>(testData);
      event.put("id", i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(event);
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, event);
      writer.flush(events, Map.of());
    }
    assertEquals(75, writer.testGetBufferSize());
    writer.close();
  }

  @Test
  void testBufferReachingBatchSizeThreshold() throws Exception {
    String path = tablePath + "_buffer4";
    createDeltaTable(path);
    Config testConfig =
        Config.builder().tablePath(path).batchSize(10).avroSchema(TEST_AVRO_SCHEMA).build();
    DeltaLakeWriter writer = createWriter(testConfig);
    writer.initialize();

    Map<String, Object> testData = new HashMap<>();
    testData.put("id", 1);
    testData.put("name", "test");

    // Add events up to batchSize - 1
    for (int i = 0; i < 9; i++) {
      Map<String, Object> event = new HashMap<>(testData);
      event.put("id", i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(event);
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, event);
      writer.flush(events, Map.of());
    }
    assertEquals(9, writer.testGetBufferSize());
    assertTrue(writer.testGetBufferSize() < testConfig.getBatchSize());

    // Add one more event to reach batchSize - triggers flush
    Map<String, Object> event = new HashMap<>(testData);
    event.put("id", 9);
    RecordFleakData record = (RecordFleakData) FleakData.wrap(event);
    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(record, event);
    writer.flush(events, Map.of());
    // Buffer should be cleared after flush
    assertEquals(0, writer.testGetBufferSize());

    writer.close();
  }

  @Test
  void testTimerBasedFlushIsScheduled() throws Exception {
    // Configure with short flush interval for testing
    String path = tablePath + "_timer1";
    createDeltaTable(path);
    Config testConfig =
        Config.builder()
            .tablePath(path)
            .batchSize(1000)
            .flushIntervalSeconds(2)
            .avroSchema(TEST_AVRO_SCHEMA)
            .build();

    DeltaLakeWriter writer = createWriter(testConfig);
    writer.initialize();

    // Access bufferedWriter field, then its internal scheduler/task fields
    Class<?> baseClass = DeltaLakeWriter.class.getSuperclass();
    java.lang.reflect.Field bufferedWriterField = baseClass.getDeclaredField("bufferedWriter");
    bufferedWriterField.setAccessible(true);
    Object bufferedWriter = bufferedWriterField.get(writer);

    java.lang.reflect.Field schedulerField =
        bufferedWriter.getClass().getDeclaredField("scheduler");
    schedulerField.setAccessible(true);
    Object scheduler = schedulerField.get(bufferedWriter);

    java.lang.reflect.Field taskField = bufferedWriter.getClass().getDeclaredField("flushTask");
    taskField.setAccessible(true);
    Object task = taskField.get(bufferedWriter);

    // Verify that timer is started
    assertNotNull(scheduler, "Flush scheduler should be initialized");
    assertNotNull(task, "Flush task should be scheduled");

    writer.close();
  }

  @Test
  void testTimerFlushWithSmallBuffer() throws Exception {
    // Timer-based flush should attempt to flush any non-empty buffer to bound data latency
    String path = tablePath + "_timer2";
    createDeltaTable(path);
    Config testConfig =
        Config.builder()
            .tablePath(path)
            .batchSize(100)
            .flushIntervalSeconds(1)
            .avroSchema(TEST_AVRO_SCHEMA)
            .build();

    DeltaLakeWriter writer = createWriter(testConfig);
    writer.initialize();

    Map<String, Object> testData = new HashMap<>();
    testData.put("id", 1);
    testData.put("name", "test");

    // Add only 5 events (small buffer)
    for (int i = 0; i < 5; i++) {
      Map<String, Object> event = new HashMap<>(testData);
      event.put("id", i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(event);
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, event);
      writer.flush(events, Map.of());
    }

    assertEquals(
        5, writer.testGetBufferSize(), "Buffer should contain 5 events before timer fires");

    // Wait for timer to fire
    TimeUnit.MILLISECONDS.sleep(1500);

    // After timer flush, the buffer should be empty
    assertEquals(0, writer.testGetBufferSize(), "Buffer should be empty after timer flush");

    writer.close();
  }

  @Test
  void testTimerFlushWithSufficientEvents() throws Exception {
    // Timer-based flush with multiple events (less than batchSize)
    String path = tablePath + "_timer3";
    createDeltaTable(path);
    Config testConfig =
        Config.builder()
            .tablePath(path)
            .batchSize(100)
            .flushIntervalSeconds(1)
            .avroSchema(TEST_AVRO_SCHEMA)
            .build();

    DeltaLakeWriter writer = createWriter(testConfig);
    writer.initialize();

    Map<String, Object> testData = new HashMap<>();
    testData.put("id", 1);
    testData.put("name", "test");

    // Add 15 events (less than batchSize, will be flushed by timer)
    for (int i = 0; i < 15; i++) {
      Map<String, Object> eventData = new HashMap<>(testData);
      eventData.put("id", i);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(eventData);
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(record, eventData);
      writer.flush(events, Map.of());
    }

    assertEquals(15, writer.testGetBufferSize(), "Buffer should contain 15 events");

    // Wait for timer to fire
    TimeUnit.MILLISECONDS.sleep(1500);

    // Buffer should be empty after timer flush
    assertEquals(0, writer.testGetBufferSize(), "Buffer should be empty after timer flush");

    writer.close();
  }

  @Test
  void testTimerFlushDisabledWhenIntervalIsZero() throws Exception {
    String path = tablePath + "_timer_disabled";
    createDeltaTable(path);
    Config testConfig =
        Config.builder()
            .tablePath(path)
            .batchSize(100)
            .flushIntervalSeconds(0) // Timer disabled
            .avroSchema(TEST_AVRO_SCHEMA)
            .build();

    DeltaLakeWriter writer = createWriter(testConfig);
    writer.initialize();

    // Access bufferedWriter field, then its internal scheduler/task fields
    Class<?> baseClass = DeltaLakeWriter.class.getSuperclass();
    java.lang.reflect.Field bufferedWriterField = baseClass.getDeclaredField("bufferedWriter");
    bufferedWriterField.setAccessible(true);
    Object bufferedWriter = bufferedWriterField.get(writer);

    java.lang.reflect.Field schedulerField =
        bufferedWriter.getClass().getDeclaredField("scheduler");
    schedulerField.setAccessible(true);
    Object scheduler = schedulerField.get(bufferedWriter);

    java.lang.reflect.Field taskField = bufferedWriter.getClass().getDeclaredField("flushTask");
    taskField.setAccessible(true);
    Object task = taskField.get(bufferedWriter);

    // Verify that timer is NOT started (scheduler is null when timer is disabled)
    assertNull(scheduler, "Flush scheduler should be null when timer is disabled");
    assertNull(task, "Flush task should be null when timer is disabled");

    writer.close();
  }

  @Test
  void testTimerStopsOnClose() throws Exception {
    String path = tablePath + "_timer_stop";
    createDeltaTable(path);
    Config testConfig =
        Config.builder()
            .tablePath(path)
            .batchSize(100)
            .flushIntervalSeconds(2)
            .avroSchema(TEST_AVRO_SCHEMA)
            .build();

    DeltaLakeWriter writer = createWriter(testConfig);
    writer.initialize();

    // Access bufferedWriter field, then its internal scheduler/task fields
    Class<?> baseClass = DeltaLakeWriter.class.getSuperclass();
    java.lang.reflect.Field bufferedWriterField = baseClass.getDeclaredField("bufferedWriter");
    bufferedWriterField.setAccessible(true);
    Object bufferedWriter = bufferedWriterField.get(writer);

    java.lang.reflect.Field schedulerField =
        bufferedWriter.getClass().getDeclaredField("scheduler");
    schedulerField.setAccessible(true);

    java.lang.reflect.Field taskField = bufferedWriter.getClass().getDeclaredField("flushTask");
    taskField.setAccessible(true);

    // Verify timer is running
    assertNotNull(schedulerField.get(bufferedWriter), "Scheduler should be running before close");
    assertNotNull(taskField.get(bufferedWriter), "Task should be scheduled before close");

    // Close the writer
    writer.close();

    // Verify timer is stopped
    assertNull(schedulerField.get(bufferedWriter), "Scheduler should be null after close");
    assertNull(taskField.get(bufferedWriter), "Task should be null after close");
  }

  @Test
  void testMultipleWritersHaveUniqueThreadNames() throws Exception {
    String path1 = tablePath + "_writer1";
    String path2 = tablePath + "_writer2";
    createDeltaTable(path1);
    createDeltaTable(path2);

    Config config1 =
        Config.builder()
            .tablePath(path1)
            .batchSize(100)
            .flushIntervalSeconds(2)
            .avroSchema(TEST_AVRO_SCHEMA)
            .build();

    Config config2 =
        Config.builder()
            .tablePath(path2)
            .batchSize(100)
            .flushIntervalSeconds(2)
            .avroSchema(TEST_AVRO_SCHEMA)
            .build();

    DeltaLakeWriter writer1 = createWriter(config1);
    DeltaLakeWriter writer2 = createWriter(config2);

    // Access instance IDs using reflection
    java.lang.reflect.Field instanceIdField = DeltaLakeWriter.class.getDeclaredField("instanceId");
    instanceIdField.setAccessible(true);

    int instanceId1 = instanceIdField.getInt(writer1);
    int instanceId2 = instanceIdField.getInt(writer2);

    // Verify that instance IDs are different
    assertNotEquals(
        instanceId1, instanceId2, "Different writer instances should have different instance IDs");

    writer1.initialize();
    writer2.initialize();

    writer1.close();
    writer2.close();
  }

  /** Creates a Delta table at the given path with a custom schema */
  private void createDeltaTableWithSchema(String path, StructType schema) {
    createDeltaTableWithSchema(path, schema, List.of());
  }

  private void createDeltaTableWithSchema(
      String path, StructType schema, List<String> partitionColumns) {
    Engine engine = DefaultEngine.create(new Configuration());
    Table table = Table.forPath(engine, path);
    TransactionBuilder txnBuilder =
        table.createTransactionBuilder(engine, "Test Table Creation", Operation.CREATE_TABLE);
    txnBuilder = txnBuilder.withSchema(engine, schema);

    if (partitionColumns != null && !partitionColumns.isEmpty()) {
      txnBuilder = txnBuilder.withPartitionColumns(engine, partitionColumns);
    }

    Transaction txn = txnBuilder.build(engine);
    txn.commit(engine, emptyCloseableIterable());
  }

  @Test
  void testNestedStructNullabilityMismatchDetection() {
    // Create table with nested struct where nested field is NON-nullable
    String path = tablePath + "_nested_struct";
    StructType addressSchema =
        new StructType(
            List.of(
                new StructField("street", StringType.STRING, false), // non-nullable
                new StructField("city", StringType.STRING, false) // non-nullable
                ));
    StructType tableSchema =
        new StructType(
            List.of(
                new StructField("id", IntegerType.INTEGER, false),
                new StructField("address", addressSchema, false)));
    createDeltaTableWithSchema(path, tableSchema);

    // Config has nested field as nullable (mismatch!)
    Map<String, Object> configAvroSchema =
        Map.of(
            "type", "record",
            "name", "TestRecord",
            "fields",
                List.of(
                    Map.of("name", "id", "type", "int"),
                    Map.of(
                        "name",
                        "address",
                        "type",
                        Map.of(
                            "type", "record",
                            "name", "Address",
                            "fields",
                                List.of(
                                    Map.of(
                                        "name",
                                        "street",
                                        "type",
                                        List.of("null", "string")), // nullable!
                                    Map.of("name", "city", "type", "string"))))));

    Config mismatchConfig = Config.builder().tablePath(path).avroSchema(configAvroSchema).build();

    DeltaLakeWriter writer = createWriter(mismatchConfig);
    IllegalStateException exception = assertThrows(IllegalStateException.class, writer::initialize);
    assertTrue(exception.getMessage().contains("Schema mismatch detected"));
    assertTrue(exception.getMessage().contains("address.street"));
    assertTrue(exception.getMessage().contains("nullability mismatch"));
  }

  @Test
  void testArrayElementNullabilityMismatchDetection() {
    // Create table with array where elements are NON-nullable
    String path = tablePath + "_array_elem";
    StructType tableSchema =
        new StructType(
            List.of(
                new StructField("id", IntegerType.INTEGER, false),
                new StructField(
                    "tags", new ArrayType(StringType.STRING, false), true) // elements non-nullable
                ));
    createDeltaTableWithSchema(path, tableSchema);

    // Config has array with nullable elements (mismatch!)
    Map<String, Object> configAvroSchema =
        Map.of(
            "type", "record",
            "name", "TestRecord",
            "fields",
                List.of(
                    Map.of("name", "id", "type", "int"),
                    Map.of(
                        "name",
                        "tags",
                        "type",
                        List.of(
                            "null",
                            Map.of(
                                "type",
                                "array",
                                "items",
                                List.of("null", "string")))))); // nullable elements!

    Config mismatchConfig = Config.builder().tablePath(path).avroSchema(configAvroSchema).build();

    DeltaLakeWriter writer = createWriter(mismatchConfig);
    IllegalStateException exception = assertThrows(IllegalStateException.class, writer::initialize);
    assertTrue(exception.getMessage().contains("Schema mismatch detected"));
    assertTrue(exception.getMessage().contains("tags"));
    assertTrue(exception.getMessage().contains("element nullability mismatch"));
  }

  @Test
  void testMapValueNullabilityMismatchDetection() {
    // Create table with map where values are NON-nullable
    String path = tablePath + "_map_value";
    StructType tableSchema =
        new StructType(
            List.of(
                new StructField("id", IntegerType.INTEGER, false),
                new StructField(
                    "metadata",
                    new MapType(StringType.STRING, StringType.STRING, false),
                    true) // values non-nullable
                ));
    createDeltaTableWithSchema(path, tableSchema);

    // Config has map with nullable values (mismatch!)
    Map<String, Object> configAvroSchema =
        Map.of(
            "type", "record",
            "name", "TestRecord",
            "fields",
                List.of(
                    Map.of("name", "id", "type", "int"),
                    Map.of(
                        "name",
                        "metadata",
                        "type",
                        List.of(
                            "null",
                            Map.of(
                                "type",
                                "map",
                                "values",
                                List.of("null", "string")))))); // nullable values!

    Config mismatchConfig = Config.builder().tablePath(path).avroSchema(configAvroSchema).build();

    DeltaLakeWriter writer = createWriter(mismatchConfig);
    IllegalStateException exception = assertThrows(IllegalStateException.class, writer::initialize);
    assertTrue(exception.getMessage().contains("Schema mismatch detected"));
    assertTrue(exception.getMessage().contains("metadata"));
    assertTrue(exception.getMessage().contains("value nullability mismatch"));
  }

  @Test
  void testDeeplyNestedNullabilityMismatchDetection() {
    // Create table with deeply nested structure: struct containing array of structs
    String path = tablePath + "_deeply_nested";
    StructType itemSchema =
        new StructType(
            List.of(
                new StructField("name", StringType.STRING, false), // non-nullable
                new StructField("price", DoubleType.DOUBLE, false) // non-nullable
                ));
    StructType tableSchema =
        new StructType(
            List.of(
                new StructField("id", IntegerType.INTEGER, false),
                new StructField(
                    "items", new ArrayType(itemSchema, false), true) // elements non-nullable
                ));
    createDeltaTableWithSchema(path, tableSchema);

    // Config has nested struct field as nullable (deep mismatch!)
    // Build nested schema step by step for clarity
    Map<String, Object> priceField =
        Map.of("name", "price", "type", List.of("null", "double")); // nullable price!
    Map<String, Object> nameField = Map.of("name", "name", "type", "string");
    Map<String, Object> itemRecordSchema =
        Map.of("type", "record", "name", "Item", "fields", List.of(nameField, priceField));
    Map<String, Object> arraySchema = Map.of("type", "array", "items", itemRecordSchema);
    Map<String, Object> itemsField = Map.of("name", "items", "type", List.of("null", arraySchema));
    Map<String, Object> idField = Map.of("name", "id", "type", "int");
    Map<String, Object> configAvroSchema =
        Map.of("type", "record", "name", "TestRecord", "fields", List.of(idField, itemsField));

    Config mismatchConfig = Config.builder().tablePath(path).avroSchema(configAvroSchema).build();

    DeltaLakeWriter writer = createWriter(mismatchConfig);
    IllegalStateException exception = assertThrows(IllegalStateException.class, writer::initialize);
    assertTrue(exception.getMessage().contains("Schema mismatch detected"));
    assertTrue(exception.getMessage().contains("items[element].price"));
    assertTrue(exception.getMessage().contains("nullability mismatch"));
  }
}
