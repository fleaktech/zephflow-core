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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.sparkrunner.SparkSchemas;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for BufferedSparkSourceExecutor. */
class BufferedSparkSourceExecutorTest {

  private static SparkSession spark;

  @BeforeAll
  static void setUp() {
    spark =
        SparkSession.builder()
            .appName("BufferedSparkSourceExecutorTest")
            .master("local[2]")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void testExecuteWithBatchSource() throws Exception {
    TestBatchSourceCommand sourceCommand = new TestBatchSourceCommand(3);
    MetricClientProvider metricProvider = new MetricClientProvider.NoopMetricClientProvider();

    BufferedSparkSourceExecutor executor =
        BufferedSparkSourceExecutor.builder()
            .metricClientProvider(metricProvider)
            .jobId("test-job")
            .build();

    Dataset<Row> result = executor.execute(sourceCommand, spark);

    assertNotNull(result);
    assertEquals(SparkSchemas.INPUT_EVENT_SCHEMA, result.schema());

    List<Row> rows = result.collectAsList();
    assertEquals(3, rows.size());

    for (int i = 0; i < 3; i++) {
      Row row = rows.get(i);
      scala.collection.Map<String, org.apache.spark.unsafe.types.VariantVal> dataMap =
          row.getMap(0);
      assertTrue(dataMap.contains("key" + i));
    }
  }

  @Test
  void testExecuteWithEmptySource() throws Exception {
    TestBatchSourceCommand sourceCommand = new TestBatchSourceCommand(0);
    MetricClientProvider metricProvider = new MetricClientProvider.NoopMetricClientProvider();

    BufferedSparkSourceExecutor executor =
        BufferedSparkSourceExecutor.builder()
            .metricClientProvider(metricProvider)
            .jobId("test-job")
            .build();

    Dataset<Row> result = executor.execute(sourceCommand, spark);

    assertNotNull(result);
    assertEquals(SparkSchemas.INPUT_EVENT_SCHEMA, result.schema());
    assertEquals(0, result.count());
  }

  @Test
  void testExecuteWithMultipleBatches() throws Exception {
    TestBatchSourceCommand sourceCommand = new TestBatchSourceCommand(10);
    MetricClientProvider metricProvider = new MetricClientProvider.NoopMetricClientProvider();

    BufferedSparkSourceExecutor executor =
        BufferedSparkSourceExecutor.builder()
            .metricClientProvider(metricProvider)
            .jobId("test-job")
            .build();

    Dataset<Row> result = executor.execute(sourceCommand, spark);

    assertNotNull(result);
    assertEquals(10, result.count());
  }

  /** Test source command that produces a fixed number of records. */
  static class TestBatchSourceCommand
      extends io.fleak.zephflow.lib.commands.source.SimpleSourceCommand<RecordFleakData> {
    private final int recordCount;

    TestBatchSourceCommand(int recordCount) {
      super(
          "test-source",
          JobContext.builder().metricTags(Map.of()).build(),
          new TestConfigParser(),
          new TestConfigValidator(),
          true); // single event source
      this.recordCount = recordCount;
    }

    @Override
    public SourceType sourceType() {
      return SourceType.BATCH;
    }

    @Override
    public String commandName() {
      return "testBatchSource";
    }

    @Override
    protected io.fleak.zephflow.lib.commands.source.SourceExecutionContext<RecordFleakData>
        createExecutionContext(
            MetricClientProvider metricClientProvider,
            JobContext jobContext,
            CommandConfig commandConfig,
            String nodeId) {

      io.fleak.zephflow.lib.commands.source.Fetcher<RecordFleakData> fetcher =
          new io.fleak.zephflow.lib.commands.source.Fetcher<>() {
            private boolean fetched = false;

            @Override
            public List<RecordFleakData> fetch() {
              if (fetched) {
                return List.of();
              }
              fetched = true;

              List<RecordFleakData> allRecords = new ArrayList<>();
              for (int i = 0; i < recordCount; i++) {
                allRecords.add(
                    (RecordFleakData)
                        io.fleak.zephflow.api.structure.FleakData.wrap(
                            Map.of("key" + i, "value" + i)));
              }
              return allRecords;
            }

            @Override
            public void close() {}
          };

      io.fleak.zephflow.lib.commands.source.RawDataConverter<RecordFleakData> converter =
          (sourceRecord, config) ->
              io.fleak.zephflow.lib.commands.source.ConvertedResult.success(
                  List.of(sourceRecord), sourceRecord);

      io.fleak.zephflow.lib.commands.source.RawDataEncoder<RecordFleakData> encoder =
          sourceRecord ->
              new io.fleak.zephflow.lib.serdes.SerializedEvent(new byte[0], new byte[0], Map.of());

      return new io.fleak.zephflow.lib.commands.source.SourceExecutionContext<>(
          fetcher,
          converter,
          encoder,
          metricClientProvider.counter("test_input_size", Map.of()),
          metricClientProvider.counter("test_input_count", Map.of()),
          metricClientProvider.counter("test_deser_err", Map.of()),
          null);
    }
  }

  static class TestConfigParser implements ConfigParser {
    @Override
    public CommandConfig parseConfig(Map<String, Object> rawConfig) {
      return null;
    }
  }

  static class TestConfigValidator implements ConfigValidator {
    @Override
    public void validateConfig(CommandConfig config, String nodeId, JobContext jobContext) {}
  }
}
