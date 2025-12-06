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
package io.fleak.zephflow.runner.dag;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonResource;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.YamlUtils.fromYamlResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.commands.noop.NoopConfigParser;
import io.fleak.zephflow.lib.commands.noop.NoopConfigValidator;
import io.fleak.zephflow.lib.commands.sink.PassThroughMessagePreProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.runner.DagExecutor;
import io.fleak.zephflow.runner.JobConfig;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Created by bolei on 2/28/25 */
public class DagExecutorTest {

  public static final Map<String, CommandFactory> COMMAND_FACTORY_MAP =
      new HashMap<>(OperatorCommandRegistry.OPERATOR_COMMANDS);
  public static final Map<String, Set<Map<String, Object>>> IN_MEM_SINK = new HashMap<>();

  @BeforeEach
  public void setup() {
    IN_MEM_SINK.clear();
  }

  @Test
  void testExecuteDag() throws Exception {

    /*
     *                               /->(even) c
     * a -> b(calculate type odd/even)
     *                               \->(odd)  d
     * */
    executeDag("/test_dag.yml", "/expected_output.json");
  }

  @Test
  void testExecuteDag2() throws Exception {
    /*
     *  /->(even events)->(add even tag) b-\
     * a-->(odd events) ->(add odd tag)  c -> d
     *  \->        (all events)           -/
     * */
    executeDag("/test_dag_2.yml", "/expected_output_2.json");
  }

  @Test
  /*
   * a -> b(num times 2) -> c
   * */
  void testExecuteDag3() throws Exception {
    executeDag("/test_dag_linear_dag.yml", "/expected_output_linear_dag.json");
  }

  private void executeDag(String dagResource, String expectedOutputResource) throws Exception {
    AdjacencyListDagDefinition adjacencyListDagDefinition =
        fromYamlResource(dagResource, new TypeReference<>() {});

    COMMAND_FACTORY_MAP.put("testSource", new TestSourceFactory());
    COMMAND_FACTORY_MAP.put("testSink", new TestSinkFactory());
    DagExecutor dagExecutor =
        DagExecutor.createDagExecutor(
            JobConfig.builder()
                .dagDefinition(adjacencyListDagDefinition)
                .jobId("test_job")
                .environment("test_env")
                .service("test_service")
                .build(),
            COMMAND_FACTORY_MAP,
            new MetricClientProvider.NoopMetricClientProvider());
    dagExecutor.executeDag();
    Map<String, Set<Map<String, Object>>> expected =
        fromJsonResource(expectedOutputResource, new TypeReference<>() {});
    assertEquals(FleakData.wrap(expected), FleakData.wrap(IN_MEM_SINK));
  }

  static class TestSource extends SimpleSourceCommand<RecordFleakData> {

    protected TestSource(
        String nodeId,
        JobContext jobContext,
        ConfigParser configParser,
        ConfigValidator configValidator) {
      super(nodeId, jobContext, configParser, configValidator, true);
    }

    @Override
    public SourceType sourceType() {
      return SourceType.BATCH;
    }

    @Override
    public String commandName() {
      return "testSource";
    }

    @Override
    protected SourceExecutionContext<RecordFleakData> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      // Create fetcher that generates 10 test events
      Fetcher<RecordFleakData> fetcher =
          new Fetcher<>() {
            @Override
            public List<RecordFleakData> fetch() {
              int eventCount = 10;
              List<RecordFleakData> sourceEvents = new ArrayList<>();
              for (int i = 0; i < eventCount; ++i) {
                sourceEvents.add((RecordFleakData) FleakData.wrap(Map.of("num", i)));
              }
              return sourceEvents;
            }

            @Override
            public void close() {}
          };

      // Create converter that passes through RecordFleakData
      RawDataConverter<RecordFleakData> converter =
          (sourceRecord, config) -> ConvertedResult.success(List.of(sourceRecord), sourceRecord);

      // Create encoder that converts to JSON bytes
      RawDataEncoder<RecordFleakData> encoder =
          sourceRecord -> {
            String str = toJsonString(sourceRecord);
            assert str != null;
            return new SerializedEvent(null, str.getBytes(), null);
          };

      return new SourceExecutionContext<>(
          fetcher,
          converter,
          encoder,
          metricClientProvider.counter("input_event_size_count", Map.of()),
          metricClientProvider.counter("input_event_count", Map.of()),
          metricClientProvider.counter("input_deser_err_count", Map.of()),
          null);
    }
  }

  static class TestSourceFactory extends CommandFactory {
    @Override
    public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
      return new TestSource(nodeId, jobContext, new NoopConfigParser(), new NoopConfigValidator());
    }

    @Override
    public CommandType commandType() {
      return CommandType.SOURCE;
    }
  }

  public static class TestSink extends SimpleSinkCommand<RecordFleakData> {

    protected TestSink(
        String nodeId,
        JobContext jobContext,
        ConfigParser configParser,
        ConfigValidator configValidator) {
      super(nodeId, jobContext, configParser, configValidator);
    }

    @Override
    protected int batchSize() {
      return 1;
    }

    @Override
    public String commandName() {
      return "testSink";
    }

    @Override
    protected SinkExecutionContext<RecordFleakData> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      // Create flusher that writes to IN_MEM_SINK
      SimpleSinkCommand.Flusher<RecordFleakData> flusher =
          new SimpleSinkCommand.Flusher<>() {
            @Override
            public SimpleSinkCommand.FlushResult flush(
                SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents,
                Map<String, String> metricTags) {
              Set<Map<String, Object>> sink =
                  IN_MEM_SINK.computeIfAbsent(nodeId, k -> new HashSet<>());
              sink.addAll(
                  preparedInputEvents.preparedList().stream()
                      .map(RecordFleakData::unwrap)
                      .toList());
              return new SimpleSinkCommand.FlushResult(
                  preparedInputEvents.preparedList().size(), 0, List.of());
            }

            @Override
            public void close() {}
          };

      SimpleSinkCommand.SinkMessagePreProcessor<RecordFleakData> preprocessor =
          new PassThroughMessagePreProcessor();

      return new SinkExecutionContext<>(
          flusher,
          preprocessor,
          metricClientProvider.counter("input_event_count", Map.of()),
          metricClientProvider.counter("error_event_count", Map.of()),
          metricClientProvider.counter("sink_output_count", Map.of()),
          metricClientProvider.counter("output_event_size_count", Map.of()),
          metricClientProvider.counter("sink_error_count", Map.of()));
    }
  }

  static class TestSinkFactory extends CommandFactory {

    @Override
    public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
      return new TestSink(nodeId, jobContext, new NoopConfigParser(), new NoopConfigValidator());
    }

    @Override
    public CommandType commandType() {
      return CommandType.SINK;
    }
  }
}
