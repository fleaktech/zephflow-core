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
package io.fleak.zephflow.sdk;

import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static io.fleak.zephflow.runner.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.FleakStopWatch;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.utils.JsonUtils;
import io.fleak.zephflow.lib.utils.YamlUtils;
import io.fleak.zephflow.runner.DagCompilationException;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZephFlowTest {
  static final List<Map<String, Object>> SOURCE_EVENTS = new ArrayList<>();

  static {
    for (int i = 0; i < 10; ++i) {
      SOURCE_EVENTS.add(Map.of("num", i));
    }
  }

  private InputStream in;
  private ByteArrayOutputStream testOut;
  private PrintStream psOut;

  @BeforeEach
  public void setup() {
    in = new ByteArrayInputStream(Objects.requireNonNull(toJsonString(SOURCE_EVENTS)).getBytes());
    testOut = new ByteArrayOutputStream();
    psOut = new PrintStream(testOut);
    System.setIn(in);
    System.setOut(psOut);
  }

  @AfterEach
  public void teardown() {
    try {
      in.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    psOut.close();
  }

  /**
   * Test case: Simple filtering and transformation Flow pattern: Single input source → filter →
   * transform → output
   */
  @Test
  public void testFilterAndTransform() throws Exception {
    // Create a simple flow to filter numbers > 5 and add additional properties
    ZephFlow flow = ZephFlow.startFlow();

    // Create input flow
    ZephFlow inputFlow = flow.stdinSource(EncodingType.JSON_ARRAY);

    // Filter and transform
    ZephFlow processedFlow =
        inputFlow
            .filter("$.num > 5")
            .eval("dict(original=$.num, doubled=$.num*2, description='High value')");

    // Output to stdout
    ZephFlow outputFlow = processedFlow.stdoutSink(EncodingType.JSON_OBJECT);

    // Run the test
    runTestWithStdIO(outputFlow, "/expected_output_filter_transform.json");
  }

  @Test
  public void testExecuteDag() throws Exception {
    /*
     *  /->(even events)->(add even tag) b-\
     *a--->(odd events) ->(add odd tag)  c -> d
     *  \->        (all events)           -/
     */

    ZephFlow flow = ZephFlow.startFlow();

    ZephFlow inputFlow = flow.stdinSource(EncodingType.JSON_ARRAY);

    // Create even flow: filter for even numbers, then add even tag
    ZephFlow evenFlow =
        inputFlow
            .filter("$.num%2 == 0") // Apply condition for even numbers
            .eval("dict(type='even', num=$.num)");

    // Create odd flow: filter for odd numbers, then add odd tag
    ZephFlow oddFlow =
        inputFlow
            .filter("$.num%2 == 1") // Apply condition for odd numbers
            .sql("SELECT num, 'odd' AS type FROM events;");

    // Direct connection from input to output for all events
    // No condition needed for this path

    // Merge all three flows: evenFlow, oddFlow, and inputFlow
    ZephFlow mergedFlow = ZephFlow.merge(evenFlow, oddFlow, inputFlow);
    // Connect to stdout sink
    ZephFlow outputFlow = mergedFlow.stdoutSink(EncodingType.JSON_OBJECT);
    runTestWithStdIO(outputFlow, "/expected_output_stdio.json");
  }

  /**
   * Test case: Complex nested data transformations Flow pattern: Input → multiple nested
   * transformations → output
   */
  @Test
  public void testComplexTransformations() throws Exception {
    ZephFlow flow = ZephFlow.startFlow();

    // Create input flow
    ZephFlow inputFlow = flow.stdinSource(EncodingType.JSON_ARRAY);

    // Apply multiple transformations in sequence
    ZephFlow transformedFlow =
        inputFlow
            .eval("dict(value=$.num, category=case($.num%2==0 => 'even', _ => 'odd'))")
            .eval(
                "dict(origValue=$.value, category=$.category, valueRange=case($.value<3 => 'low', $.value<7 => 'medium', _ => 'high'))")
            .eval(
                "dict(summary=dict(value=$.origValue, category=$.category, valueRange=$.valueRange), meta=dict(processed=true))");

    // Output to stdout
    ZephFlow outputFlow = transformedFlow.stdoutSink(EncodingType.JSON_OBJECT);

    // Run the test
    runTestWithStdIO(outputFlow, "/expected_output_complex_transformations.json");
  }

  /**
   * Test case: Conditional branching, processing, and aggregation Flow pattern: Input → branch by
   * range (low/medium/high) → process each branch differently → merge → output
   */
  @Test
  public void testConditionalBranchingAndAggregation() throws Exception {
    ZephFlow flow = ZephFlow.startFlow();

    // Create input flow
    ZephFlow inputFlow = flow.stdinSource(EncodingType.JSON_ARRAY);

    // Branch into low/medium/high value streams
    ZephFlow lowFlow =
        inputFlow.filter("$.num < 3").eval("dict(value=$.num, category='low', factor=1.1)");

    ZephFlow mediumFlow =
        inputFlow
            .filter("$.num >= 3 and $.num < 7")
            .eval("dict(value=$.num, category='medium', factor=1.5)");

    ZephFlow highFlow =
        inputFlow.filter("$.num >= 7").eval("dict(value=$.num, category='high', factor=2.0)");

    // Merge the flows
    ZephFlow mergedFlow = ZephFlow.merge(lowFlow, mediumFlow, highFlow);

    // Process after merging
    ZephFlow processedFlow =
        mergedFlow.eval("dict(original=$.value, category=$.category, adjusted=$.value * $.factor)");

    // Output to stdout
    ZephFlow outputFlow = processedFlow.stdoutSink(EncodingType.JSON_OBJECT);

    // Run the test
    runTestWithStdIO(outputFlow, "/expected_output_conditional_branching.json");
  }

  /** Test case: Branch after merge */
  @Test
  public void testMergeWithConditionalDownstream() throws Exception {
    ZephFlow flow = ZephFlow.startFlow();

    // Create input flow
    ZephFlow inputFlow = flow.stdinSource(EncodingType.JSON_ARRAY);

    // Process input in two different ways
    ZephFlow processA = inputFlow.eval("dict(source='A', value=$.num * 2 + 1)");
    ZephFlow processB = inputFlow.eval("dict(source='B', value=$.num * 2)");

    // Merge the differently processed flows
    ZephFlow merged = ZephFlow.merge(processA, processB);

    // Add conditional downstream processing
    ZephFlow highScoreFlow =
        merged
            .filter("$.value > 10")
            .eval("dict(source=$.source, value=$.value, highPriority=true)");

    ZephFlow lowScoreFlow =
        merged
            .filter("$.value <= 10")
            .eval("dict(source=$.source, value=$.value, highPriority=false)");

    // Merge all downstream flows
    ZephFlow finalMerged = ZephFlow.merge(highScoreFlow, lowScoreFlow);

    // Output to stdout
    ZephFlow outputFlow = finalMerged.stdoutSink(EncodingType.JSON_OBJECT);

    // Run the test
    runTestWithStdIO(outputFlow, "/expected_output_merge_branch.json");
  }

  /**
   * Test case: Nested conditional processing Flow pattern: Input → branch → nested branch → merge →
   * output
   */
  @Test
  public void testNestedConditionalProcessing() throws Exception {
    ZephFlow flow = ZephFlow.startFlow();

    // Create input flow
    ZephFlow inputFlow = flow.stdinSource(EncodingType.JSON_ARRAY);

    // First level branching: by range
    ZephFlow lowRange = inputFlow.filter("$.num < 5");
    ZephFlow highRange = inputFlow.filter("$.num >= 5");

    // Second level branching for low range: by evenness
    ZephFlow lowEven =
        lowRange.filter("$.num % 2 == 0").eval("dict(value=$.num, valueRange='low', category='A')");

    ZephFlow lowOdd =
        lowRange.filter("$.num % 2 == 1").eval("dict(value=$.num, valueRange='low', category='B')");

    // Second level branching for high range: by divisibility by 3
    ZephFlow highDiv3 =
        highRange
            .filter("$.num % 3 == 0")
            .eval("dict(value=$.num, valueRange='high', category='C')");

    ZephFlow highNotDiv3 =
        highRange
            .filter("$.num % 3 != 0")
            .eval("dict(value=$.num, valueRange='high', category='D')");

    // Merge all leaf paths
    ZephFlow mergedFlow = ZephFlow.merge(lowEven, lowOdd, highDiv3, highNotDiv3);

    // Add final processing
    ZephFlow finalFlow =
        mergedFlow.eval(
            "dict(original=$.value, metadata=dict(valueRange=$.valueRange, category=$.category))");

    // Output to stdout
    ZephFlow outputFlow = finalFlow.stdoutSink(EncodingType.JSON_OBJECT);

    // Run the test
    runTestWithStdIO(outputFlow, "/expected_output_nested_conditional.json");
  }

  @Test
  public void testAssertion() throws Exception {
    MetricClientProvider metricClientProvider = mock();
    FleakCounter assertionInputMessageCounter = mock();
    FleakCounter stdoutInputMessageCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_INPUT_EVENT_COUNT), any()))
        .then(
            i -> {
              Map<String, String> metricTags = i.getArgument(1);
              String cmdName = metricTags.get(METRIC_TAG_COMMAND_NAME);
              if (COMMAND_NAME_ASSERTION.equals(cmdName)) {
                return assertionInputMessageCounter;
              } else if (COMMAND_NAME_STDOUT.equals(cmdName)) {
                return stdoutInputMessageCounter;
              } else {
                return mock(FleakCounter.class);
              }
            });
    FleakCounter assertionOutputMessageCounter = mock();

    when(metricClientProvider.counter(eq(METRIC_NAME_OUTPUT_EVENT_COUNT), any()))
        .then(
            i -> {
              Map<String, String> metricTags = i.getArgument(1);
              String cmdName = metricTags.get(METRIC_TAG_COMMAND_NAME);
              if (COMMAND_NAME_ASSERTION.equals(cmdName)) {
                return assertionOutputMessageCounter;
              } else {
                fail();
                return null;
              }
            });
    when(metricClientProvider.counter(eq(METRIC_NAME_INPUT_EVENT_SIZE_COUNT), any()))
        .thenReturn(mock());
    when(metricClientProvider.counter(eq(METRIC_NAME_INPUT_DESER_ERR_COUNT), any()))
        .thenReturn(mock());
    FleakCounter assertionErrorCounter = mock();
    FleakCounter stdoutErrorMessageCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_ERROR_EVENT_COUNT), any()))
        .then(
            i -> {
              Map<String, String> metricTags = i.getArgument(1);
              String cmdName = metricTags.get(METRIC_TAG_COMMAND_NAME);
              if (COMMAND_NAME_ASSERTION.equals(cmdName)) {
                return assertionErrorCounter;
              } else if (COMMAND_NAME_STDOUT.equals(cmdName)) {
                return stdoutErrorMessageCounter;
              } else {
                fail();
                return null;
              }
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

    FleakCounter outputSizeCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_OUTPUT_EVENT_SIZE_COUNT), any()))
        .thenReturn(outputSizeCounter);

    FleakCounter errorEventCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_PIPELINE_ERROR_EVENT), any()))
        .thenReturn(errorEventCounter);

    FleakStopWatch stopWatch = mock();
    when(metricClientProvider.stopWatch(eq(METRIC_NAME_REQUEST_PROCESS_TIME_MILLIS), any()))
        .thenReturn(stopWatch);

    // dag: stdin -> assert -> stdout; no dlq configuration
    // input from stdin has 5 event numbers and 5 odd numbers. assertion rule: num%2==0
    // assertion command should throw 5 errors and output 5 good events
    // stdout command should receive 5 good events and shouldn't encounter any errors
    ZephFlow flow = ZephFlow.startFlow(metricClientProvider);
    var assertionFlow = flow.stdinSource(EncodingType.JSON_ARRAY).assertion("$.num%2==0");
    var outputFlow = assertionFlow.stdoutSink(EncodingType.JSON_OBJECT);
    outputFlow.execute("test_id", "test_env", "test_service");

    // assertion node counters
    verify(assertionInputMessageCounter, times(10)).increase(any());
    verify(assertionOutputMessageCounter, times(5)).increase(any());
    verify(assertionErrorCounter, times(5)).increase(any());

    // stdout node counters
    verify(stdoutInputMessageCounter, times(5)).increase(eq(1L), any());
    verify(stdoutErrorMessageCounter, never()).increase(any());
    verify(sinkOutputCounter, times(5)).increase(eq(1L), any());
    verify(sinkErrorCounter, times(5)).increase(eq(0L), any());

    verify(inputEventCounter).increase(eq(10L), any());
    verify(outputEventCounter).increase(eq(5L), any());
    verify(errorEventCounter).increase(eq(5L), any());
  }

  @Test
  public void testDisconnectedPaths() {
    // Create the flow with the disconnected paths pattern
    ZephFlow flow = ZephFlow.startFlow();
    ZephFlow evenPath = flow.filter("$.num%2 == 0").eval("dict(num=$.num, label='even')");
    ZephFlow oddPath = flow.filter("$.num%2 == 1").eval("dict(num=$.num, label='odd')");

    // Build the DAG
    AdjacencyListDagDefinition dag = ZephFlow.merge(evenPath, oddPath).buildDag();

    // Get the nodes from the DAG
    List<AdjacencyListDagDefinition.DagNode> nodes = dag.getDag();

    // There should be 4 nodes: 2 filters and 2 evals
    assertEquals(4, nodes.size());

    // Create a map of node ID to node for easier lookup
    Map<String, AdjacencyListDagDefinition.DagNode> nodeMap =
        nodes.stream()
            .collect(Collectors.toMap(AdjacencyListDagDefinition.DagNode::getId, node -> node));

    // Create a map to track nodes by command name
    Map<String, List<AdjacencyListDagDefinition.DagNode>> nodesByCommand = new HashMap<>();

    // Group nodes by command name
    for (AdjacencyListDagDefinition.DagNode node : nodes) {
      nodesByCommand.computeIfAbsent(node.getCommandName(), k -> new ArrayList<>()).add(node);
    }

    // There should be 2 filter nodes
    List<AdjacencyListDagDefinition.DagNode> filterNodes = nodesByCommand.get("filter");
    assertEquals(2, filterNodes.size());

    // There should be 2 eval nodes
    List<AdjacencyListDagDefinition.DagNode> evalNodes = nodesByCommand.get("eval");
    assertEquals(2, evalNodes.size());

    // Identify even and odd filter nodes
    AdjacencyListDagDefinition.DagNode evenFilterNode = null;
    AdjacencyListDagDefinition.DagNode oddFilterNode = null;

    for (AdjacencyListDagDefinition.DagNode node : filterNodes) {
      if (node.getConfig().contains("$.num%2 == 0")) {
        evenFilterNode = node;
      } else if (node.getConfig().contains("$.num%2 == 1")) {
        oddFilterNode = node;
      }
    }

    assertNotNull(evenFilterNode);
    assertNotNull(oddFilterNode);

    // Verify each filter node connects to exactly one eval node
    assertEquals(1, evenFilterNode.getOutputs().size());
    assertEquals(1, oddFilterNode.getOutputs().size());

    // Get the eval nodes that each filter connects to
    String evenEvalId = evenFilterNode.getOutputs().get(0);
    String oddEvalId = oddFilterNode.getOutputs().get(0);

    AdjacencyListDagDefinition.DagNode evenEvalNode = nodeMap.get(evenEvalId);
    AdjacencyListDagDefinition.DagNode oddEvalNode = nodeMap.get(oddEvalId);

    assertNotNull(evenEvalNode);
    assertNotNull(oddEvalNode);

    // Verify the content of eval nodes
    assertTrue(evenEvalNode.getConfig().contains("dict(num=$.num, label='even')"));
    assertTrue(oddEvalNode.getConfig().contains("dict(num=$.num, label='odd')"));

    // Verify eval nodes have no outputs
    assertTrue(evenEvalNode.getOutputs().isEmpty());
    assertTrue(oddEvalNode.getOutputs().isEmpty());

    // Verify the two paths are disconnected (no node from one path connects to the other)
    assertFalse(evenFilterNode.getOutputs().contains(oddEvalId));
    assertFalse(oddFilterNode.getOutputs().contains(evenEvalId));

    // Verify these are both entry nodes (no other node connects to them)
    boolean evenFilterIsEntryNode = true;
    boolean oddFilterIsEntryNode = true;

    for (AdjacencyListDagDefinition.DagNode node : nodes) {
      if (node.getOutputs().contains(evenFilterNode.getId())) {
        evenFilterIsEntryNode = false;
      }
      if (node.getOutputs().contains(oddFilterNode.getId())) {
        oddFilterIsEntryNode = false;
      }
    }

    assertTrue(evenFilterIsEntryNode);
    assertTrue(oddFilterIsEntryNode);
  }

  @Test
  public void testSinkAfterSink() {
    ZephFlow flow =
        ZephFlow.startFlow()
            .stdinSource(EncodingType.JSON_ARRAY)
            .stdoutSink(EncodingType.JSON_OBJECT)
            .stdoutSink(EncodingType.JSON_OBJECT); // sink after sink
    DagCompilationException ex =
        assertThrows(
            DagCompilationException.class,
            () -> flow.execute("test_job_id", "test_env", "test_service"));
    System.err.println(ex.getMessage());
    assertTrue(ex.getMessage().contains("Sink nodes must be terminal nodes."));
  }

  /**
   * Test case: Source after source This should fail because a node cannot receive input from
   * multiple source nodes
   */
  @Test
  public void testSourceAfterSource() {
    ZephFlow flow =
        ZephFlow.startFlow()
            .stdinSource(EncodingType.JSON_ARRAY) // first source
            .stdinSource(EncodingType.JSON_ARRAY); // second source

    // The flow should fail during execution
    DagCompilationException ex =
        assertThrows(
            DagCompilationException.class,
            () -> flow.execute("test_job_id", "test_env", "test_service"));
    assertTrue(ex.getMessage().contains("Source nodes cannot have incoming connections"));
  }

  /** Test case: Empty flow A flow without any nodes should fail */
  @Test
  public void testEmptyFlow() {
    ZephFlow flow = ZephFlow.startFlow();
    // No source or sink specified

    // Should fail because flow needs at least a source and sink
    Exception e =
        assertThrows(
            Exception.class, () -> flow.execute("test_job_id", "test_env", "test_service"));
    assertEquals("Dag validation failed: Graph nodes must not be empty", e.getMessage());
  }

  /**
   * Test case: Invalid filter expression Should fail when an invalid expression is provided to
   * filter
   */
  @Test
  public void testInvalidFilterExpression() {
    ZephFlow flow =
        ZephFlow.startFlow()
            .stdinSource(EncodingType.JSON_ARRAY)
            .filter("this is not a valid expression") // Invalid filter expression
            .stdoutSink(EncodingType.JSON_OBJECT);

    // Should fail due to invalid filter expression
    DagCompilationException e =
        assertThrows(
            DagCompilationException.class,
            () -> flow.execute("test_job_id", "test_env", "test_service"));
    assertEquals(e.getCommandName(), COMMAND_NAME_FILTER);
    assertTrue(e.getNodeId().startsWith("filter_"));
  }

  /**
   * Test case: Multiple entry points Should detect and potentially reject a flow with multiple
   * entry points
   */
  @Test
  public void testMultipleEntryPoints() {
    // Create two separate flows starting from different sources
    ZephFlow flow1 = ZephFlow.startFlow().stdinSource(EncodingType.JSON_ARRAY);
    ZephFlow flow2 = ZephFlow.startFlow().stdinSource(EncodingType.JSON_ARRAY);

    // Merge them and add a sink
    ZephFlow mergedFlow = ZephFlow.merge(flow1, flow2);
    var outputFlow = mergedFlow.stdoutSink(EncodingType.JSON_OBJECT);

    // The merged flow has two entry points (stdin and kafka)
    // Check if this is allowed or rejected based on your architecture
    Exception e =
        assertThrows(
            Exception.class, () -> outputFlow.execute("test_job_id", "test_env", "test_service"));
    assertEquals("dag executor only supports dag with exactly one entry node", e.getMessage());
  }

  /**
   * Test case: Validate with multiple sinks Ensures that a flow with multiple sink endpoints is
   * valid
   */
  @Test
  public void testMultipleSinks() throws Exception {
    ZephFlow flow = ZephFlow.startFlow().stdinSource(EncodingType.JSON_ARRAY);

    // Branch into two different sinks
    ZephFlow stdoutSink1 = flow.filter("$.num > 5").stdoutSink(EncodingType.JSON_OBJECT);

    ZephFlow stdoutSink2 = flow.filter("$.num <= 8").stdoutSink(EncodingType.JSON_OBJECT);

    // The flow has multiple sinks (each path ends with a sink)
    // This should be valid
    ZephFlow mergedFlow = ZephFlow.merge(stdoutSink1, stdoutSink2);

    // Since we can't actually connect to Kafka in a unit test,
    // we'll just validate the DAG structure is accepted
    runTestWithStdIO(mergedFlow, "/expected_output_multiple_sinks.json");
  }

  @Test
  public void testMultipleMerge() throws Exception {
    ZephFlow inputFlow = ZephFlow.startFlow().stdinSource(EncodingType.JSON_ARRAY);
    ZephFlow evenFlow =
        inputFlow
            .filter("$.num%2 == 0") // Apply condition for even numbers
            .eval("dict(type='even', num=$.num)");

    // Create odd flow: filter for odd numbers, then add odd tag
    ZephFlow oddFlow =
        inputFlow
            .filter("$.num%2 == 1") // Apply condition for odd numbers
            .sql("SELECT num, 'odd' AS type FROM events;");
    ZephFlow merge1 = ZephFlow.merge(evenFlow, oddFlow);
    ZephFlow merge2 = ZephFlow.merge(merge1, inputFlow);
    ZephFlow outputFlow = merge2.stdoutSink(EncodingType.JSON_OBJECT);
    runTestWithStdIO(outputFlow, "/expected_output_stdio.json");
  }

  @Test
  public void testMergeAndBranch() throws Exception {
    ZephFlow inputFlow = ZephFlow.startFlow().stdinSource(EncodingType.JSON_ARRAY);
    ZephFlow merge0 =
        ZephFlow.merge(inputFlow.filter("$.num%2 == 0"), inputFlow.filter("$.num%2 == 1"));

    ZephFlow evenFlow =
        merge0
            .filter("$.num%2 == 0") // Apply condition for even numbers
            .eval("dict(type='even', num=$.num)");

    // Create odd flow: filter for odd numbers, then add odd tag
    ZephFlow oddFlow =
        merge0
            .filter("$.num%2 == 1") // Apply condition for odd numbers
            .sql("SELECT num, 'odd' AS type FROM events;");
    ZephFlow merge1 = ZephFlow.merge(evenFlow, oddFlow);
    ZephFlow merge2 = ZephFlow.merge(merge1, inputFlow);
    ZephFlow outputFlow = merge2.stdoutSink(EncodingType.JSON_OBJECT);
    runTestWithStdIO(outputFlow, "/expected_output_stdio.json");
  }

  @Test
  public void testExecuteWithYaml() throws Exception{
    ZephFlow flow = ZephFlow.startFlow();

    ZephFlow inputFlow = flow.stdinSource(EncodingType.JSON_ARRAY);
    ZephFlow outputFlow = inputFlow.stdoutSink(EncodingType.JSON_OBJECT);
    AdjacencyListDagDefinition dagDefinition = outputFlow.buildDag();

    String dagStr = YamlUtils.toYamlString(dagDefinition);
    assertNotNull(dagStr);
    ZephFlow.executeYamlDag("test_id", "test_env", "test_service", dagStr, null);
    String output = testOut.toString();
    assertTrue(output.contains("{\"num\":0}"));
  }

  @Test
  public void testExecuteWithJson() throws Exception{
    ZephFlow flow = ZephFlow.startFlow();

    ZephFlow inputFlow = flow.stdinSource(EncodingType.JSON_ARRAY);
    ZephFlow outputFlow = inputFlow.stdoutSink(EncodingType.JSON_OBJECT);
    AdjacencyListDagDefinition dagDefinition = outputFlow.buildDag();

    String dagStr = JsonUtils.toJsonString(dagDefinition);
    assertNotNull(dagStr);
    ZephFlow.executeJsonDag("test_id", "test_env", "test_service", dagStr, null);
    String output = testOut.toString();
    assertTrue(output.contains("{\"num\":0}"));
  }

  private void runTestWithStdIO(ZephFlow outputFlow, String expectedOutputResource)
      throws Exception {
    outputFlow.execute("test_id", "test_env", "test_service");
    String output = testOut.toString();
    List<String> lines = output.lines().toList();
    var objects =
        lines.stream()
            .filter(l -> l.startsWith("{"))
            .map(l -> fromJsonString(l, new TypeReference<Map<String, Object>>() {}))
            .collect(Collectors.toSet());
    Set<Map<String, Object>> expected =
        fromJsonResource(expectedOutputResource, new TypeReference<>() {});
    assertEquals(expected, objects);
  }
}
