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
import io.fleak.zephflow.lib.commands.sink.SinkCommandInitializerFactory;
import io.fleak.zephflow.lib.commands.sink.SinkCommandPartsFactory;
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
        ConfigValidator configValidator,
        CommandInitializerFactory commandInitializerFactory) {
      super(nodeId, jobContext, configParser, configValidator, commandInitializerFactory, true);
    }

    @Override
    public SourceType sourceType() {
      return SourceType.BATCH;
    }

    @Override
    public String commandName() {
      return "testSource";
    }
  }

  static class TestSourceFactory extends CommandFactory {
    @Override
    public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
      return new TestSource(
          nodeId,
          jobContext,
          new NoopConfigParser(),
          new NoopConfigValidator(),
          new SourceCommandInitializerFactory<RecordFleakData>() {
            @Override
            protected CommandPartsFactory createCommandPartsFactory(
                MetricClientProvider metricClientProvider,
                JobContext jobContext,
                CommandConfig commandConfig,
                String nodeId) {
              return new SourceCommandPartsFactory<RecordFleakData>(metricClientProvider) {
                @Override
                public Fetcher<RecordFleakData> createFetcher(CommandConfig commandConfig) {
                  return new Fetcher<>() {
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
                }

                @Override
                public RawDataConverter<RecordFleakData> createRawDataConverter(
                    CommandConfig commandConfig) {
                  return (sourceRecord, config) ->
                      ConvertedResult.success(List.of(sourceRecord), sourceRecord);
                }

                @Override
                public RawDataEncoder<RecordFleakData> createRawDataEncoder(
                    CommandConfig commandConfig) {
                  return sourceRecord -> {
                    String str = toJsonString(sourceRecord);
                    assert str != null;
                    return new SerializedEvent(null, str.getBytes(), null);
                  };
                }
              };
            }
          });
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
        ConfigValidator configValidator,
        SinkCommandInitializerFactory<RecordFleakData> sinkCommandInitializerFactory) {
      super(nodeId, jobContext, configParser, configValidator, sinkCommandInitializerFactory);
    }

    @Override
    protected int batchSize() {
      return 1;
    }

    @Override
    public String commandName() {
      return "testSink";
    }
  }

  static class TestSinkFactory extends CommandFactory {

    @Override
    public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
      return new TestSink(
          nodeId,
          jobContext,
          new NoopConfigParser(),
          new NoopConfigValidator(),
          new SinkCommandInitializerFactory<>() {
            @Override
            protected CommandPartsFactory createCommandPartsFactory(
                MetricClientProvider metricClientProvider,
                JobContext jobContext,
                CommandConfig commandConfig,
                String nodeId) {
              return new SinkCommandPartsFactory<RecordFleakData>(
                  metricClientProvider, jobContext) {
                @Override
                public SimpleSinkCommand.SinkMessagePreProcessor<RecordFleakData>
                    createMessagePreProcessor() {
                  return new PassThroughMessagePreProcessor();
                }

                @Override
                public SimpleSinkCommand.Flusher<RecordFleakData> createFlusher() {
                  return new SimpleSinkCommand.Flusher<>() {
                    @Override
                    public SimpleSinkCommand.FlushResult flush(
                        SimpleSinkCommand.PreparedInputEvents<RecordFleakData>
                            preparedInputEvents) {
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
                }
              };
            }
          });
    }

    @Override
    public CommandType commandType() {
      return CommandType.SINK;
    }
  }
}
