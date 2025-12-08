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
package io.fleak.zephflow.lib.commands.sink;

import static io.fleak.zephflow.lib.utils.JsonUtils.loadFleakDataFromJsonResource;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Created by bolei on 4/19/24 */
class SimpleSinkCommandTest {
  private final String currentNodeId = "test_node";
  private final JobContext jobContext =
      JobContext.builder()
          .metricTags(
              Map.of(
                  METRIC_TAG_SERVICE, "my_service",
                  METRIC_TAG_ENV, "my_env"))
          .build();

  private final String callingUser = "test_user";

  private MetricClientProvider metricClientProvider;
  private FleakCounter inputMessageCounter;
  private FleakCounter errorCounter;
  private FleakCounter sinkOutputCounter;
  private FleakCounter outputSizeCounter;
  private FleakCounter sinkErrorCounter;

  @BeforeEach
  public void beforeEach() {
    metricClientProvider = mock();
    inputMessageCounter = mock();
    errorCounter = mock();
    sinkOutputCounter = mock();
    outputSizeCounter = mock();
    sinkErrorCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_INPUT_EVENT_COUNT), any()))
        .thenReturn(inputMessageCounter);
    when(metricClientProvider.counter(eq(METRIC_NAME_ERROR_EVENT_COUNT), any()))
        .thenReturn(errorCounter);
    when(metricClientProvider.counter(eq(METRIC_NAME_SINK_OUTPUT_COUNT), any()))
        .thenReturn(sinkOutputCounter);
    when(metricClientProvider.counter(eq(METRIC_NAME_OUTPUT_EVENT_SIZE_COUNT), any()))
        .thenReturn(outputSizeCounter);
    when(metricClientProvider.counter(eq(METRIC_NAME_SINK_ERROR_COUNT), any()))
        .thenReturn(sinkErrorCounter);
  }

  /**
   *
   *
   * <pre>
   * This test sends 5 events (0,1,2,3,4) to a fake sink.
   * This sink will
   *   - fail to preprocess events (1, 4)
   *   - fail to flush event (2)
   *   - succeed to flush (0,, 3)
   *
   * After the `writeToSink` method call, also check the counters report the correct metrics
   * </pre>
   *
   * @throws Exception report any error
   */
  @Test
  public void testWriteToSink() throws Exception {
    FakeSimpleSinkCommand sinkCommand =
        (FakeSimpleSinkCommand)
            new FakeSimpleSinkCommandFactory().createCommand(currentNodeId, jobContext);
    sinkCommand.parseAndValidateArg(null);
    FleakData fleakData = loadFleakDataFromJsonResource("/json/multiple_simple_events.json");
    List<RecordFleakData> input =
        fleakData.getArrayPayload().stream().map(fd -> (RecordFleakData) fd).toList();
    sinkCommand.initialize(metricClientProvider);
    var context = sinkCommand.getExecutionContext();
    ScalarSinkCommand.SinkResult sinkResult = sinkCommand.writeToSink(input, callingUser, context);
    ScalarSinkCommand.SinkResult expected =
        new ScalarSinkCommand.SinkResult(
            5,
            2,
            List.of(
                new ErrorOutput(
                    new RecordFleakData(
                        Map.of(
                            "val",
                            new NumberPrimitiveFleakData(
                                1, NumberPrimitiveFleakData.NumberType.LONG))),
                    "process error"),
                new ErrorOutput(
                    new RecordFleakData(
                        Map.of(
                            "val",
                            new NumberPrimitiveFleakData(
                                2, NumberPrimitiveFleakData.NumberType.LONG))),
                    "flush error"),
                new ErrorOutput(
                    new RecordFleakData(
                        Map.of(
                            "val",
                            new NumberPrimitiveFleakData(
                                4, NumberPrimitiveFleakData.NumberType.LONG))),
                    "process error")));
    assertEquals(expected, sinkResult);
    ArgumentCaptor<Long> inputMessageCounterCaptor = ArgumentCaptor.forClass(Long.class);
    verify(inputMessageCounter, times(2)).increase(inputMessageCounterCaptor.capture(), any());
    assertEquals(List.of(3L, 2L), inputMessageCounterCaptor.getAllValues());
    verify(errorCounter, times(2)).increase(any());
    verify(sinkOutputCounter, times(2)).increase(eq(1L), any());
    verify(outputSizeCounter, times(2)).increase(eq(100L), anyMap());
    verify(sinkErrorCounter).increase(eq(1L), any());
  }

  @Test
  public void testWriteToSink_allFailOnPreprocessing() throws Exception {
    FakeSimpleSinkCommand sinkCommand =
        (FakeSimpleSinkCommand)
            new FakeSimpleSinkCommandFactory().createCommand(currentNodeId, jobContext);
    sinkCommand.parseAndValidateArg(null);
    FleakData fleakData = loadFleakDataFromJsonResource("/json/multiple_simple_events.json");
    List<RecordFleakData> input =
        fleakData.getArrayPayload().stream()
            .map(fd -> (RecordFleakData) fd)
            .filter(fd -> ((int) fd.getPayload().get("val").getNumberValue()) % 3 == 1)
            .toList();
    sinkCommand.initialize(metricClientProvider);
    var context = sinkCommand.getExecutionContext();
    ScalarSinkCommand.SinkResult sinkResult = sinkCommand.writeToSink(input, callingUser, context);
    ScalarSinkCommand.SinkResult expected =
        new ScalarSinkCommand.SinkResult(
            2,
            0,
            List.of(
                new ErrorOutput(
                    new RecordFleakData(
                        Map.of(
                            "val",
                            new NumberPrimitiveFleakData(
                                1, NumberPrimitiveFleakData.NumberType.LONG))),
                    "process error"),
                new ErrorOutput(
                    new RecordFleakData(
                        Map.of(
                            "val",
                            new NumberPrimitiveFleakData(
                                4, NumberPrimitiveFleakData.NumberType.LONG))),
                    "process error")));
    assertEquals(expected, sinkResult);
    verify(errorCounter, times(2)).increase(any());
    verify(outputSizeCounter, never()).increase(anyLong(), any());
  }

  private static class FakeSimpleSinkCommandFactory extends CommandFactory {

    @Override
    public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
      return new FakeSimpleSinkCommand(
          nodeId,
          jobContext,
          (ConfigParser) configStr -> new CommandConfig() {},
          (ConfigValidator) (commandConfig, nodeId1, jobContext1) -> {});
    }

    @Override
    public CommandType commandType() {
      return CommandType.SINK;
    }
  }

  private static class FakeSimpleSinkCommand extends SimpleSinkCommand<Integer> {

    protected FakeSimpleSinkCommand(
        String nodeId,
        JobContext jobContext,
        ConfigParser configParser,
        ConfigValidator configValidator) {
      super(nodeId, jobContext, configParser, configValidator);
    }

    @Override
    protected int batchSize() {
      return 3;
    }

    @Override
    public String commandName() {
      return "fake_sink";
    }

    @Override
    protected SinkExecutionContext<Integer> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      SimpleSinkCommand.SinkMessagePreProcessor<Integer> preprocessor =
          (event, ts) -> {
            int val = (int) event.getPayload().get("val").getNumberValue();
            if (val % 3 == 1) {
              throw new RuntimeException("process error");
            }
            return val;
          };

      SimpleSinkCommand.Flusher<Integer> flusher = new FakeSimpleFlusher();

      return new SinkExecutionContext<>(
          flusher,
          preprocessor,
          metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, jobContext.getMetricTags()),
          metricClientProvider.counter(METRIC_NAME_ERROR_EVENT_COUNT, jobContext.getMetricTags()),
          metricClientProvider.counter(METRIC_NAME_SINK_OUTPUT_COUNT, jobContext.getMetricTags()),
          metricClientProvider.counter(
              METRIC_NAME_OUTPUT_EVENT_SIZE_COUNT, jobContext.getMetricTags()),
          metricClientProvider.counter(METRIC_NAME_SINK_ERROR_COUNT, jobContext.getMetricTags()));
    }
  }

  private static class FakeSimpleFlusher implements SimpleSinkCommand.Flusher<Integer> {

    @Override
    public SimpleSinkCommand.FlushResult flush(
        SimpleSinkCommand.PreparedInputEvents<Integer> preparedInputEvents,
        Map<String, String> metricTags) {
      int successSize = 0;
      List<ErrorOutput> errorOutputList = new ArrayList<>();
      for (Pair<RecordFleakData, Integer> pair : preparedInputEvents.rawAndPreparedList()) {
        if (pair.getValue() % 3 != 0) {
          errorOutputList.add(new ErrorOutput(pair.getKey(), "flush error"));
          continue;
        }
        successSize++;
      }
      return new SimpleSinkCommand.FlushResult(successSize, 100, errorOutputList);
    }

    @Override
    public void close() {}
  }
}
