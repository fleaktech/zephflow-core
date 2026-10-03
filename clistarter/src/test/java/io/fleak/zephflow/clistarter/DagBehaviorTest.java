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
package io.fleak.zephflow.clistarter;

import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static io.fleak.zephflow.runner.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.FleakStopWatch;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.runner.DagExecutor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class DagBehaviorTest {

  private static final List<Map<String, Object>> SOURCE_EVENTS =
      LongStream.range(0, 10).<Map<String, Object>>mapToObj(i -> Map.of("num", i)).toList();

  @Test
  void testFilterAndTransform() throws Exception {
    assertStdoutEvents(
        expectedEvents("/expected_output_filter_transform.json"),
        runMain("/test_dag_filter_transform.yml"));
  }

  @Test
  void testEvenOddBranchesWithSqlMergedWithInput() throws Exception {
    assertStdoutEvents(expectedStdioEvents(), runMain("/test_dag_even_odd_sql.yml"));
  }

  @Test
  void testComplexTransformations() throws Exception {
    assertStdoutEvents(
        expectedEvents("/expected_output_complex_transformations.json"),
        runMain("/test_dag_complex_transformations.yml"));
  }

  @Test
  void testConditionalBranchingAndAggregation() throws Exception {
    assertStdoutEvents(
        expectedEvents("/expected_output_conditional_branching.json"),
        runMain("/test_dag_conditional_branching.yml"));
  }

  @Test
  void testMergeWithConditionalDownstream() throws Exception {
    assertStdoutEvents(
        expectedEvents("/expected_output_merge_branch.json"),
        runMain("/test_dag_merge_branch.yml"));
  }

  @Test
  void testNestedConditionalProcessing() throws Exception {
    assertStdoutEvents(
        expectedEvents("/expected_output_nested_conditional.json"),
        runMain("/test_dag_nested_conditional.yml"));
  }

  @Test
  void testMultipleSinks() throws Exception {
    assertStdoutEvents(
        expectedEvents("/expected_output_multiple_sinks.json"),
        runMain("/test_dag_multiple_sinks.yml"));
  }

  @Test
  void testMergeAndBranch() throws Exception {
    assertStdoutEvents(expectedStdioEvents(), runMain("/test_dag_merge_and_branch.yml"));
  }

  @Test
  void testExecuteWithYaml() throws Exception {
    assertStdoutEvents(SOURCE_EVENTS, runMain("/test_dag_stdin_stdout.yml"));
  }

  @Test
  void testExecuteWithJson() throws Exception {
    assertStdoutEvents(SOURCE_EVENTS, runMain("/test_dag_stdin_stdout.json"));
  }

  // Main cannot take a MetricClientProvider, so this drives the same JobCliParser -> DagExecutor
  // path Main uses, with a mocked provider to observe per-command counters.
  @Test
  void testAssertion() throws Exception {
    MetricClientProvider metricClientProvider = mock();
    FleakCounter assertionInputMessageCounter = mock();
    FleakCounter stdoutInputMessageCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_INPUT_EVENT_COUNT), any()))
        .then(
            i ->
                switch ((String)
                    i.<Map<String, String>>getArgument(1).get(METRIC_TAG_COMMAND_NAME)) {
                  case COMMAND_NAME_ASSERTION -> assertionInputMessageCounter;
                  case COMMAND_NAME_STDOUT -> stdoutInputMessageCounter;
                  case null, default -> mock(FleakCounter.class);
                });
    FleakCounter assertionOutputMessageCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_OUTPUT_EVENT_COUNT), any()))
        .then(
            i -> {
              assertEquals(
                  COMMAND_NAME_ASSERTION,
                  i.<Map<String, String>>getArgument(1).get(METRIC_TAG_COMMAND_NAME));
              return assertionOutputMessageCounter;
            });
    when(metricClientProvider.counter(eq(METRIC_NAME_INPUT_EVENT_SIZE_COUNT), any()))
        .thenReturn(mock());
    when(metricClientProvider.counter(eq(METRIC_NAME_INPUT_DESER_ERR_COUNT), any()))
        .thenReturn(mock());
    FleakCounter assertionErrorCounter = mock();
    FleakCounter stdoutErrorMessageCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_ERROR_EVENT_COUNT), any()))
        .then(
            i ->
                switch ((String)
                    i.<Map<String, String>>getArgument(1).get(METRIC_TAG_COMMAND_NAME)) {
                  case COMMAND_NAME_ASSERTION -> assertionErrorCounter;
                  case COMMAND_NAME_STDOUT -> stdoutErrorMessageCounter;
                  case null, default -> fail("unexpected error counter: " + i.getArgument(1));
                });
    FleakCounter sinkOutputCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_SINK_OUTPUT_COUNT), any()))
        .thenReturn(sinkOutputCounter);
    FleakCounter sinkErrorCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_SINK_ERROR_COUNT), any()))
        .thenReturn(sinkErrorCounter);
    FleakCounter inputEventCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_PIPELINE_INPUT_EVENT), any()))
        .thenReturn(inputEventCounter);
    FleakCounter outputEventCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_PIPELINE_OUTPUT_EVENT), any()))
        .thenReturn(outputEventCounter);
    when(metricClientProvider.counter(eq(METRIC_NAME_OUTPUT_EVENT_SIZE_COUNT), any()))
        .thenReturn(mock());
    FleakCounter errorEventCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_PIPELINE_ERROR_EVENT), any()))
        .thenReturn(errorEventCounter);
    FleakStopWatch stopWatch = mock();
    when(metricClientProvider.stopWatch(eq(METRIC_NAME_REQUEST_PROCESS_TIME_MILLIS), any()))
        .thenReturn(stopWatch);

    String output =
        runWithStdio(
            () ->
                DagExecutor.createDagExecutor(
                        JobCliParser.parseArgs(cliArgs("/test_dag_assertion.yml")),
                        metricClientProvider)
                    .executeDag());

    assertStdoutEvents(
        SOURCE_EVENTS.stream().filter(e -> (long) e.get("num") % 2 == 0).toList(), output);

    verify(assertionInputMessageCounter, times(10)).increase(any());
    verify(assertionOutputMessageCounter, times(5)).increase(any());
    verify(assertionErrorCounter, times(5)).increase(any());

    verify(stdoutInputMessageCounter, times(5)).increase(eq(1L), any());
    verify(stdoutErrorMessageCounter, never()).increase(any());
    verify(sinkOutputCounter, times(5)).increase(eq(1L), any());
    verify(sinkErrorCounter, never()).increase(anyLong(), any());

    verify(inputEventCounter).increase(eq(10L), any());
    verify(outputEventCounter).increase(eq(5L), any());
    verify(errorEventCounter).increase(eq(5L), any());
  }

  private static String runMain(String dagResource) throws Exception {
    return runWithStdio(() -> Main.main(cliArgs(dagResource)));
  }

  private static String[] cliArgs(String dagResource) {
    String dagDefBase64Str = toBase64String(loadStringFromResource(dagResource).getBytes());
    return new String[] {
      "-d", dagDefBase64Str, "-id", "test_job", "-s", "my_service", "-e", "my_env"
    };
  }

  private static String runWithStdio(DagRun dagRun) throws Exception {
    InputStream originalIn = System.in;
    PrintStream originalOut = System.out;
    try (InputStream in =
            new ByteArrayInputStream(
                Objects.requireNonNull(toJsonString(SOURCE_EVENTS)).getBytes());
        ByteArrayOutputStream testOut = new ByteArrayOutputStream();
        PrintStream psOut = new PrintStream(testOut)) {
      System.setIn(in);
      System.setOut(psOut);
      dagRun.run();
      psOut.flush();
      return testOut.toString();
    } finally {
      System.setIn(originalIn);
      System.setOut(originalOut);
    }
  }

  private static List<Map<String, Object>> expectedEvents(String resource) throws IOException {
    return fromJsonResource(resource, new TypeReference<>() {});
  }

  private static List<Map<String, Object>> expectedStdioEvents() throws IOException {
    Map<String, List<Map<String, Object>>> bySink =
        fromJsonResource("/expected_output_stdio.json", new TypeReference<>() {});
    return bySink.get("d");
  }

  private static void assertStdoutEvents(List<Map<String, Object>> expected, String output) {
    Stream<Map<String, Object>> actual =
        output
            .lines()
            .filter(l -> l.startsWith("{\""))
            .map(l -> fromJsonString(l, new TypeReference<Map<String, Object>>() {}));
    assertEquals(
        countOccurrences(
            expected.stream()
                .map(
                    e ->
                        fromJsonString(
                            toJsonString(e), new TypeReference<Map<String, Object>>() {}))),
        countOccurrences(actual));
  }

  private static Map<Map<String, Object>, Long> countOccurrences(
      Stream<Map<String, Object>> events) {
    return events.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
  }

  @FunctionalInterface
  private interface DagRun {
    void run() throws Exception;
  }
}
