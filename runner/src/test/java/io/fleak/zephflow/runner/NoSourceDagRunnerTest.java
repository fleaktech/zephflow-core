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
package io.fleak.zephflow.runner;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static io.fleak.zephflow.runner.DagResult.sinkResultToOutputEvent;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.runner.dag.Dag;
import io.fleak.zephflow.runner.dag.Edge;
import io.fleak.zephflow.runner.dag.Node;
import java.util.*;
import org.apache.commons.collections4.MapUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.MDC;

class NoSourceDagRunnerTest {

  private static final String CALLING_USER = "test-user";
  private static final String SOURCE_NODE_ID = "source";
  private static final String NODE_ID_1 = "node1";
  private static final String NODE_ID_2 = "node2";
  private static final String SINK_ID = "sink1";
  private static final String CMD_NAME_1 = "command1";
  private static final String CMD_NAME_2 = "command2";
  private static final String CMD_NAME_3 = "command3";
  private static final String SINK_CMD_NAME = "sinkCommand";

  private MetricClientProvider mockMetricProvider;
  private DagRunCounters mockCounters;
  private ScalarCommand mockScalarCmd1;
  private ScalarCommand mockScalarCmd2;
  private ScalarSinkCommand mockSinkCmd;
  private OperatorCommand mockUnsupportedCommand;

  // Captors remain the same
  private ArgumentCaptor<List<RecordFleakData>> eventListCaptor;
  private ArgumentCaptor<Map<String, String>> tagsCaptor;

  // Test subject
  private NoSourceDagRunner noSourceDagRunner;

  // Common test data
  private List<Edge> edgesFromSource;
  private List<RecordFleakData> inputEvents;
  private Map<String, String> callingUserTag;
  private NoSourceDagRunner.DagRunConfig runConfigIncludeAll;
  private NoSourceDagRunner.DagRunConfig runConfigExcludeSteps;

  // Helper method to create dummy RecordFleakData
  private RecordFleakData createEvent(String id) {
    return new RecordFleakData(Map.of("id", Objects.requireNonNull(FleakData.wrap(id))));
  }

  // Helper method to create dummy ErrorOutput
  private ErrorOutput createError(RecordFleakData event, String message) {
    return new ErrorOutput(event, message);
  }

  @BeforeEach
  void setUp() {
    // Initialize Mocks for dependencies and commands
    mockMetricProvider = mock(MetricClientProvider.class);
    mockCounters = mock(DagRunCounters.class);
    mockScalarCmd1 = mock(ScalarCommand.class);
    mockScalarCmd2 = mock(ScalarCommand.class);
    ScalarCommand mockScalarCmd3 = mock(ScalarCommand.class);
    mockSinkCmd = mock(ScalarSinkCommand.class);
    mockUnsupportedCommand = mock(OperatorCommand.class);

    //noinspection unchecked
    eventListCaptor = ArgumentCaptor.forClass(List.class);
    //noinspection unchecked
    tagsCaptor = ArgumentCaptor.forClass(Map.class);

    // Common setup for most tests
    edgesFromSource = List.of(Edge.builder().from(SOURCE_NODE_ID).to(NODE_ID_1).build());
    inputEvents = List.of(createEvent("event1"), createEvent("event2"));
    callingUserTag = getCallingUserTagAndEventTags(CALLING_USER, null);
    runConfigIncludeAll = new NoSourceDagRunner.DagRunConfig(true, true);
    runConfigExcludeSteps = new NoSourceDagRunner.DagRunConfig(false, false);

    // Default mock behavior for commands
    lenient().when(mockScalarCmd1.commandName()).thenReturn(CMD_NAME_1);
    lenient().when(mockScalarCmd2.commandName()).thenReturn(CMD_NAME_2);
    lenient().when(mockScalarCmd3.commandName()).thenReturn(CMD_NAME_3);
    lenient().when(mockSinkCmd.commandName()).thenReturn(SINK_CMD_NAME);
    lenient()
        .when(mockUnsupportedCommand.commandName())
        .thenReturn("unsupportedCmd"); // Name for unsupported

    // Mock initialize() to do nothing (it returns void now)
    ExecutionContext mockContext = mock(ExecutionContext.class);
    lenient().doNothing().when(mockScalarCmd1).initialize(any());
    lenient().doNothing().when(mockScalarCmd2).initialize(any());
    lenient().doNothing().when(mockScalarCmd3).initialize(any());
    lenient().doNothing().when(mockSinkCmd).initialize(any());

    // Mock getExecutionContext() to return a dummy ExecutionContext
    lenient().when(mockScalarCmd1.getExecutionContext()).thenReturn(mockContext);
    lenient().when(mockScalarCmd2.getExecutionContext()).thenReturn(mockContext);
    lenient().when(mockScalarCmd3.getExecutionContext()).thenReturn(mockContext);
    lenient().when(mockSinkCmd.getExecutionContext()).thenReturn(mockContext);

    lenient()
        .when(mockScalarCmd1.process(anyList(), eq(CALLING_USER), any(ExecutionContext.class)))
        .thenAnswer(
            invocation -> {
              List<RecordFleakData> events = invocation.getArgument(0);
              return new ScalarCommand.ProcessResult(
                  new ArrayList<>(events), Collections.emptyList());
            });
    lenient()
        .when(mockScalarCmd2.process(anyList(), eq(CALLING_USER), any(ExecutionContext.class)))
        .thenAnswer(
            invocation -> {
              List<RecordFleakData> events = invocation.getArgument(0);
              return new ScalarCommand.ProcessResult(
                  new ArrayList<>(events), Collections.emptyList());
            });
    lenient()
        .when(mockScalarCmd3.process(anyList(), eq(CALLING_USER), any(ExecutionContext.class)))
        .thenAnswer(
            invocation -> {
              List<RecordFleakData> events = invocation.getArgument(0);
              return new ScalarCommand.ProcessResult(
                  new ArrayList<>(events), Collections.emptyList());
            });
    lenient()
        .when(mockSinkCmd.writeToSink(anyList(), eq(CALLING_USER), any(ExecutionContext.class)))
        .thenAnswer(
            invocation -> {
              List<RecordFleakData> events = invocation.getArgument(0);
              return new ScalarSinkCommand.SinkResult(
                  events.size(), events.size(), Collections.emptyList());
            });

    // Clear MDC before each test
    MDC.clear();
  }

  // Removed setupDagNodes and setupDagEdges helpers as we now build real DAGs

  private Map<String, String> createNodeTags(String nodeId, String commandName) {
    Map<String, String> tags = new HashMap<>(callingUserTag);
    tags.put(METRIC_TAG_NODE_ID, nodeId);
    tags.put(METRIC_TAG_COMMAND_NAME, commandName);
    return tags;
  }

  // Helper to assert nested map structure for DagResult debug info (remains useful)
  private <T> void assertDebugInfo(
      Map<String, Map<String, List<T>>> debugMap,
      String currentNodeId,
      String upstreamNodeId,
      List<T> expectedData,
      String mapType) {
    assertTrue(
        debugMap.containsKey(currentNodeId),
        mapType + " should contain key for current node ID: " + currentNodeId);
    Map<String, List<T>> upstreamMap = debugMap.get(currentNodeId);
    assertTrue(
        upstreamMap.containsKey(upstreamNodeId),
        mapType
            + " for node "
            + currentNodeId
            + " should contain key for upstream node ID: "
            + upstreamNodeId);
    assertEquals(
        expectedData,
        upstreamMap.get(upstreamNodeId),
        mapType + " data mismatch for node " + currentNodeId + " from upstream " + upstreamNodeId);
  }

  @Nested
  @DisplayName("Run Method Tests")
  class RunTests {

    @Test
    @DisplayName("should process linear DAG (Scalar -> Sink) successfully")
    void run_shouldProcessLinearDagWithScalarAndSink_whenSuccessful() {

      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> sinkNode =
          Node.<OperatorCommand>builder().id(SINK_ID).nodeContent(mockSinkCmd).build();
      List<Node<OperatorCommand>> nodes = List.of(node1, sinkNode);
      List<Edge> edges = List.of(Edge.builder().from(NODE_ID_1).to(SINK_ID).build());
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges); // Create the real DAG

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, false);

      DagResult result = noSourceDagRunner.run(inputEvents, CALLING_USER, runConfigIncludeAll);

      // Verify command processing
      verify(mockScalarCmd1)
          .process(eq(inputEvents), eq(CALLING_USER), any(ExecutionContext.class));
      verify(mockSinkCmd)
          .writeToSink(eventListCaptor.capture(), eq(CALLING_USER), any(ExecutionContext.class));
      assertEquals(
          inputEvents.size(),
          eventListCaptor.getValue().size(),
          "Sink received incorrect number of events");

      // Verify counter interactions
      verify(mockCounters)
          .increaseInputEventCounter(eq((long) inputEvents.size()), eq(callingUserTag));
      verify(mockCounters).startStopWatch();
      verify(mockCounters)
          .increaseOutputEventCounter(eq((long) inputEvents.size()), tagsCaptor.capture());
      assertEquals(
          createNodeTags(SINK_ID, SINK_CMD_NAME),
          tagsCaptor.getValue(),
          "Sink output counter tags mismatch");
      verify(mockCounters).stopStopWatch(eq(callingUserTag));
      verify(mockCounters, never()).increaseErrorEventCounter(anyLong(), anyMap());

      // Verify DagResult
      assertTrue(result.errorByStep.isEmpty(), "errorByStep should be empty");
      assertDebugInfo(result.outputByStep, NODE_ID_1, SOURCE_NODE_ID, inputEvents, "outputByStep");
      RecordFleakData expectedSinkSummary =
          new RecordFleakData(
              Map.of(
                  "inputCount",
                      new NumberPrimitiveFleakData(
                          inputEvents.size(), NumberPrimitiveFleakData.NumberType.LONG),
                  "successCount",
                      new NumberPrimitiveFleakData(
                          inputEvents.size(), NumberPrimitiveFleakData.NumberType.LONG)));
      assertDebugInfo(
          result.outputByStep, SINK_ID, NODE_ID_1, List.of(expectedSinkSummary), "outputByStep");
      assertTrue(result.outputEvents.containsKey(SINK_ID), "outputEvents should contain sink ID");
      assertEquals(
          List.of(expectedSinkSummary),
          result.outputEvents.get(SINK_ID),
          "Sink outputEvents content mismatch");
      assertEquals(
          1, result.outputEvents.size(), "outputEvents should only contain the terminal sink node");
      assertNull(MDC.get("callingUser"), "MDC callingUser should be cleared after run");
    }

    @Test
    @DisplayName("should process DAG ending in non-sink node")
    void run_shouldProcessDagEndingInNonSinkNode() {

      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> node2 =
          Node.<OperatorCommand>builder().id(NODE_ID_2).nodeContent(mockScalarCmd2).build();
      List<Node<OperatorCommand>> nodes = List.of(node1, node2);
      List<Edge> edges = List.of(Edge.builder().from(NODE_ID_1).to(NODE_ID_2).build());
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges); // Create the real DAG

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, false);

      DagResult result = noSourceDagRunner.run(inputEvents, CALLING_USER, runConfigIncludeAll);

      verify(mockScalarCmd1)
          .process(eq(inputEvents), eq(CALLING_USER), any(ExecutionContext.class));
      verify(mockScalarCmd2)
          .process(eventListCaptor.capture(), eq(CALLING_USER), any(ExecutionContext.class));
      assertEquals(inputEvents.size(), eventListCaptor.getValue().size());

      verify(mockCounters)
          .increaseOutputEventCounter(eq((long) inputEvents.size()), tagsCaptor.capture());
      assertEquals(
          createNodeTags(NODE_ID_2, CMD_NAME_2),
          tagsCaptor.getValue(),
          "Final node output counter tags mismatch");
      verify(mockCounters).stopStopWatch(eq(callingUserTag));
      verify(mockCounters, never()).increaseErrorEventCounter(anyLong(), anyMap());

      assertTrue(result.errorByStep.isEmpty(), "errorByStep should be empty");
      assertDebugInfo(result.outputByStep, NODE_ID_1, SOURCE_NODE_ID, inputEvents, "outputByStep");
      assertDebugInfo(result.outputByStep, NODE_ID_2, NODE_ID_1, inputEvents, "outputByStep");
      assertTrue(
          result.outputEvents.containsKey(NODE_ID_2), "outputEvents should contain final node ID");
      assertEquals(
          inputEvents,
          result.outputEvents.get(NODE_ID_2),
          "Final node outputEvents content mismatch");
      assertEquals(
          1, result.outputEvents.size(), "outputEvents should only contain the terminal node");
    }

    @Test
    @DisplayName("should process branching DAG (Source -> 1, Source -> 2; 1 -> Sink, 2 -> Sink)")
    void run_shouldProcessBranchingDag() {

      edgesFromSource = // Need edges from source to both starting nodes
          List.of(
              Edge.builder().from(SOURCE_NODE_ID).to(NODE_ID_1).build(),
              Edge.builder().from(SOURCE_NODE_ID).to(NODE_ID_2).build());

      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> node2 =
          Node.<OperatorCommand>builder().id(NODE_ID_2).nodeContent(mockScalarCmd2).build();
      Node<OperatorCommand> sinkNode =
          Node.<OperatorCommand>builder().id(SINK_ID).nodeContent(mockSinkCmd).build();
      List<Node<OperatorCommand>> nodes = List.of(node1, node2, sinkNode);
      List<Edge> edges =
          List.of( // Edges within the compiled DAG part
              Edge.builder().from(NODE_ID_1).to(SINK_ID).build(),
              Edge.builder().from(NODE_ID_2).to(SINK_ID).build());
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges); // Create the real DAG

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, false);

      DagResult result = noSourceDagRunner.run(inputEvents, CALLING_USER, runConfigIncludeAll);

      verify(mockScalarCmd1)
          .process(eq(inputEvents), eq(CALLING_USER), any(ExecutionContext.class));
      verify(mockScalarCmd2)
          .process(eq(inputEvents), eq(CALLING_USER), any(ExecutionContext.class));
      verify(mockSinkCmd, times(2))
          .writeToSink(eventListCaptor.capture(), eq(CALLING_USER), any(ExecutionContext.class));
      assertEquals(inputEvents.size(), eventListCaptor.getAllValues().get(0).size());
      assertEquals(inputEvents.size(), eventListCaptor.getAllValues().get(1).size());

      verify(mockCounters)
          .increaseInputEventCounter(eq((long) inputEvents.size()), eq(callingUserTag));
      verify(mockCounters).startStopWatch();
      verify(mockCounters, times(2))
          .increaseOutputEventCounter(eq((long) inputEvents.size()), tagsCaptor.capture());
      Map<String, String> expectedSinkTags = createNodeTags(SINK_ID, SINK_CMD_NAME);
      long sinkTagCount =
          tagsCaptor.getAllValues().stream().filter(expectedSinkTags::equals).count();
      assertEquals(2, sinkTagCount, "Sink node output counter tags mismatch in captures");
      verify(mockCounters).stopStopWatch(eq(callingUserTag));
      verify(mockCounters, never()).increaseErrorEventCounter(anyLong(), anyMap());

      assertTrue(result.errorByStep.isEmpty(), "errorByStep should be empty");
      assertDebugInfo(result.outputByStep, NODE_ID_1, SOURCE_NODE_ID, inputEvents, "outputByStep");
      assertDebugInfo(result.outputByStep, NODE_ID_2, SOURCE_NODE_ID, inputEvents, "outputByStep");
      RecordFleakData expectedSinkSummary =
          new RecordFleakData(
              Map.of(
                  "inputCount",
                      new NumberPrimitiveFleakData(
                          inputEvents.size(), NumberPrimitiveFleakData.NumberType.LONG),
                  "successCount",
                      new NumberPrimitiveFleakData(
                          inputEvents.size(), NumberPrimitiveFleakData.NumberType.LONG)));
      assertDebugInfo(
          result.outputByStep, SINK_ID, NODE_ID_1, List.of(expectedSinkSummary), "outputByStep");
      assertDebugInfo(
          result.outputByStep, SINK_ID, NODE_ID_2, List.of(expectedSinkSummary), "outputByStep");
      assertTrue(result.outputEvents.containsKey(SINK_ID), "outputEvents should contain Sink ID");
      var sinkResult = new ScalarSinkCommand.SinkResult(4, 4, List.of());
      var sinkOutputEvent = sinkResultToOutputEvent(sinkResult);
      assertEquals(
          List.of(sinkOutputEvent),
          result.outputEvents.get(SINK_ID),
          "Sink outputEvents missing events");
      assertEquals(
          1, result.outputEvents.size(), "outputEvents should only contain the terminal sink node");
    }

    @Test
    @DisplayName("should handle empty input event list")
    void run_shouldHandleEmptyInputEventList() {

      inputEvents = Collections.emptyList();
      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      List<Node<OperatorCommand>> nodes = List.of(node1);
      List<Edge> edges = Collections.emptyList(); // No edges after node1
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges); // Create the real DAG

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, false);

      DagResult result = noSourceDagRunner.run(inputEvents, CALLING_USER, runConfigIncludeAll);

      verify(mockScalarCmd1)
          .process(eq(Collections.emptyList()), eq(CALLING_USER), any(ExecutionContext.class));

      verify(mockCounters).increaseInputEventCounter(eq(0L), eq(callingUserTag));
      verify(mockCounters).startStopWatch();
      verify(mockCounters).increaseOutputEventCounter(eq(0L), tagsCaptor.capture());
      assertEquals(createNodeTags(NODE_ID_1, CMD_NAME_1), tagsCaptor.getValue());
      verify(mockCounters).stopStopWatch(eq(callingUserTag));
      verify(mockCounters, never()).increaseErrorEventCounter(anyLong(), anyMap());

      assertTrue(result.errorByStep.isEmpty(), "errorByStep should be empty for empty input");
      assertDebugInfo(
          result.outputByStep, NODE_ID_1, SOURCE_NODE_ID, Collections.emptyList(), "outputByStep");
      assertTrue(MapUtils.isEmpty(result.errorByStep));
      assertTrue(
          result.outputEvents.containsKey(NODE_ID_1), "outputEvents should contain final node ID");
      assertTrue(
          result.outputEvents.get(NODE_ID_1).isEmpty(),
          "outputEvents list for final node should be empty");
      assertEquals(
          1, result.outputEvents.size(), "outputEvents should only contain the terminal node");
    }

    @Test
    @DisplayName(
        "should throw IllegalArgumentException for multiple source node IDs in edgesFromSource")
    void run_shouldThrowExceptionForMultipleSourceIds() {

      edgesFromSource =
          List.of(
              Edge.builder().from(SOURCE_NODE_ID).to(NODE_ID_1).build(),
              Edge.builder().from("anotherSource").to(NODE_ID_2).build()); // Different 'from'

      // Create a minimal valid DAG for the constructor, even though it won't be used
      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> node2 =
          Node.<OperatorCommand>builder().id(NODE_ID_2).nodeContent(mockScalarCmd2).build();
      Dag<OperatorCommand> dummyDag = new Dag<>(List.of(node1, node2), Collections.emptyList());

      noSourceDagRunner =
          new NoSourceDagRunner(edgesFromSource, dummyDag, mockMetricProvider, mockCounters, false);

      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> noSourceDagRunner.run(inputEvents, CALLING_USER, runConfigIncludeAll),
              "Should throw IllegalArgumentException for multiple source IDs");

      assertEquals(
          "Only single source DAG is supported but found 2 sources", exception.getMessage());
    }

    @Test
    @DisplayName("should throw IllegalStateException for unsupported command type")
    void run_shouldThrowExceptionForUnsupportedCommand() {

      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockUnsupportedCommand).build();
      List<Node<OperatorCommand>> nodes = List.of(node1);
      List<Edge> edges = Collections.emptyList(); // Node 1 is terminal
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges); // Create the real DAG

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, false);

      IllegalStateException exception =
          assertThrows(
              IllegalStateException.class,
              () -> noSourceDagRunner.run(inputEvents, CALLING_USER, runConfigIncludeAll),
              "Should throw IllegalStateException for unsupported command");

      assertTrue(
          exception.getMessage().contains("encountered unsupported command"),
          "Exception message mismatch");
      assertTrue(
          exception.getMessage().contains(NODE_ID_1), "Exception message should contain node ID");
      assertTrue(
          exception.getMessage().contains("unsupportedCmd"),
          "Exception message should contain command name");

      verify(mockCounters)
          .increaseInputEventCounter(eq((long) inputEvents.size()), eq(callingUserTag));
      verify(mockCounters).startStopWatch();
      verify(mockCounters, never()).increaseOutputEventCounter(anyLong(), anyMap());
      verify(mockCounters, never()).increaseErrorEventCounter(anyLong(), anyMap());
    }
  }

  @Nested
  @DisplayName("Error Handling and DLQ Tests")
  class ErrorHandlingTests {

    @Test
    @DisplayName("should capture errors from ScalarCommand when useDlq=false")
    void run_shouldCaptureScalarCommandErrors_whenDlqFalse() {

      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> node2 =
          Node.<OperatorCommand>builder().id(NODE_ID_2).nodeContent(mockScalarCmd2).build();
      List<Node<OperatorCommand>> nodes = List.of(node1, node2);
      List<Edge> edges = List.of(Edge.builder().from(NODE_ID_1).to(NODE_ID_2).build());
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges);

      RecordFleakData errorEvent = createEvent("errorEvent");
      List<RecordFleakData> successfulEvents = List.of(createEvent("successEvent"));
      List<ErrorOutput> errors = List.of(createError(errorEvent, "Scalar failed"));
      when(mockScalarCmd1.process(eq(inputEvents), eq(CALLING_USER), any(ExecutionContext.class)))
          .thenReturn(new ScalarCommand.ProcessResult(successfulEvents, errors));

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource,
              compiledDag,
              mockMetricProvider,
              mockCounters,
              false); // useDlq=false

      DagResult result = noSourceDagRunner.run(inputEvents, CALLING_USER, runConfigIncludeAll);

      verify(mockScalarCmd2)
          .process(eq(successfulEvents), eq(CALLING_USER), any(ExecutionContext.class));
      verify(mockCounters)
          .increaseErrorEventCounter(eq((long) errors.size()), tagsCaptor.capture());
      assertEquals(createNodeTags(NODE_ID_1, CMD_NAME_1), tagsCaptor.getValue());
      verify(mockCounters)
          .increaseOutputEventCounter(eq((long) successfulEvents.size()), tagsCaptor.capture());
      assertEquals(createNodeTags(NODE_ID_2, CMD_NAME_2), tagsCaptor.getValue());
      assertFalse(result.errorByStep.isEmpty(), "errorByStep should not be empty");
      assertDebugInfo(result.errorByStep, NODE_ID_1, SOURCE_NODE_ID, errors, "errorByStep");
      assertDebugInfo(
          result.outputByStep, NODE_ID_1, SOURCE_NODE_ID, successfulEvents, "outputByStep");
      assertDebugInfo(result.outputByStep, NODE_ID_2, NODE_ID_1, successfulEvents, "outputByStep");
      assertTrue(
          result.outputEvents.containsKey(NODE_ID_2), "outputEvents should contain final node ID");
      assertEquals(
          successfulEvents,
          result.outputEvents.get(NODE_ID_2),
          "Final node outputEvents content mismatch");
    }

    @Test
    @DisplayName("should throw exception from ScalarCommand when useDlq=true")
    void run_shouldThrowExceptionFromScalarCommandErrors_whenDlqTrue() {

      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      List<Node<OperatorCommand>> nodes = List.of(node1);
      List<Edge> edges = Collections.emptyList(); // No edges needed as it should fail
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges);

      RecordFleakData errorEvent = createEvent("errorEvent");
      List<RecordFleakData> successfulEvents = List.of(createEvent("successEvent"));
      String errorMessage = "Scalar failed for DLQ";
      List<ErrorOutput> errors = List.of(createError(errorEvent, errorMessage));
      when(mockScalarCmd1.process(eq(inputEvents), eq(CALLING_USER), any(ExecutionContext.class)))
          .thenReturn(new ScalarCommand.ProcessResult(successfulEvents, errors));

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, true); // useDlq=true

      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> noSourceDagRunner.run(inputEvents, CALLING_USER, runConfigIncludeAll),
              "Should throw IllegalArgumentException when useDlq=true and errors occur");
      assertEquals(errorMessage, exception.getMessage(), "Exception message mismatch");
      verify(mockCounters, never()).increaseErrorEventCounter(anyLong(), anyMap());
      verify(mockScalarCmd2, never()).process(anyList(), anyString(), any());
      verify(mockCounters).increaseInputEventCounter(anyLong(), anyMap());
      verify(mockCounters).startStopWatch();
    }

    @Test
    @DisplayName("should capture errors from ScalarSinkCommand when useDlq=false")
    void run_shouldCaptureSinkCommandErrors_whenDlqFalse() {
      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> sinkNode =
          Node.<OperatorCommand>builder().id(SINK_ID).nodeContent(mockSinkCmd).build();
      List<Node<OperatorCommand>> nodes = List.of(node1, sinkNode);
      List<Edge> edges = List.of(Edge.builder().from(NODE_ID_1).to(SINK_ID).build());
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges);

      int inputCount = inputEvents.size();
      int successCount = inputCount - 1;
      List<ErrorOutput> errors = List.of(createError(inputEvents.get(1), "Sink failed"));
      var sinkResult = new ScalarSinkCommand.SinkResult(inputCount, successCount, errors);
      when(mockSinkCmd.writeToSink(eq(inputEvents), eq(CALLING_USER), any(ExecutionContext.class)))
          .thenReturn(sinkResult);

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource,
              compiledDag,
              mockMetricProvider,
              mockCounters,
              false); // useDlq=false

      DagResult result = noSourceDagRunner.run(inputEvents, CALLING_USER, runConfigIncludeAll);

      verify(mockCounters)
          .increaseErrorEventCounter(eq((long) errors.size()), tagsCaptor.capture());
      assertEquals(createNodeTags(SINK_ID, SINK_CMD_NAME), tagsCaptor.getValue());
      verify(mockCounters)
          .increaseOutputEventCounter(eq((long) inputEvents.size()), tagsCaptor.capture());
      assertEquals(createNodeTags(SINK_ID, SINK_CMD_NAME), tagsCaptor.getValue());
      assertFalse(result.errorByStep.isEmpty(), "errorByStep should not be empty");
      assertDebugInfo(result.errorByStep, SINK_ID, NODE_ID_1, errors, "errorByStep");
      RecordFleakData expectedSinkSummary =
          new RecordFleakData(
              Map.of(
                  "inputCount",
                      new NumberPrimitiveFleakData(
                          inputCount, NumberPrimitiveFleakData.NumberType.LONG),
                  "successCount",
                      new NumberPrimitiveFleakData(
                          successCount, NumberPrimitiveFleakData.NumberType.LONG)));
      assertDebugInfo(
          result.outputByStep, SINK_ID, NODE_ID_1, List.of(expectedSinkSummary), "outputByStep");
      assertTrue(result.outputEvents.containsKey(SINK_ID), "outputEvents should contain sink ID");

      var outputEvent = sinkResultToOutputEvent(sinkResult);

      assertEquals(
          List.of(outputEvent),
          result.outputEvents.get(SINK_ID),
          "Sink outputEvents content mismatch");
    }

    @Test
    @DisplayName("should throw exception from ScalarSinkCommand when useDlq=true")
    void run_shouldThrowExceptionFromSinkCommandErrors_whenDlqTrue() {
      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> sinkNode =
          Node.<OperatorCommand>builder().id(SINK_ID).nodeContent(mockSinkCmd).build();
      List<Node<OperatorCommand>> nodes = List.of(node1, sinkNode);
      List<Edge> edges = List.of(Edge.builder().from(NODE_ID_1).to(SINK_ID).build());
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges);

      int inputCount = inputEvents.size();
      int successCount = inputCount - 1;
      String errorMessage = "Sink failed for DLQ";
      List<ErrorOutput> errors = List.of(createError(inputEvents.get(1), errorMessage));
      var sinkResult = new ScalarSinkCommand.SinkResult(inputCount, successCount, errors);
      when(mockSinkCmd.writeToSink(eq(inputEvents), eq(CALLING_USER), any(ExecutionContext.class)))
          .thenReturn(sinkResult);

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, true); // useDlq=true

      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> noSourceDagRunner.run(inputEvents, CALLING_USER, runConfigIncludeAll),
              "Should throw IllegalArgumentException when useDlq=true and sink errors occur");
      assertEquals(errorMessage, exception.getMessage(), "Exception message mismatch");
      verify(mockCounters, never()).increaseErrorEventCounter(anyLong(), anyMap());
      verify(mockCounters).increaseInputEventCounter(anyLong(), anyMap());
      verify(mockCounters).startStopWatch();
    }

    @Test
    @DisplayName("should respect DagRunConfig for including errors when useDlq=false")
    void run_shouldRespectDagRunConfigForErrors_whenDlqFalse() {
      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      List<Node<OperatorCommand>> nodes = List.of(node1);
      List<Edge> edges = Collections.emptyList();
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges);

      List<ErrorOutput> errors = List.of(createError(createEvent("errorEvent"), "Scalar failed"));
      when(mockScalarCmd1.process(anyList(), anyString(), any()))
          .thenReturn(new ScalarCommand.ProcessResult(Collections.emptyList(), errors));

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource,
              compiledDag,
              mockMetricProvider,
              mockCounters,
              false); // useDlq=false

      DagResult result =
          noSourceDagRunner.run(
              inputEvents, CALLING_USER, runConfigExcludeSteps); // includeErrorByStep=false

      verify(mockCounters).increaseErrorEventCounter(eq((long) errors.size()), anyMap());
      assertTrue(
          result.errorByStep.isEmpty(),
          "errorByStep map should be empty when includeErrorByStep=false");
    }
  }

  @Nested
  @DisplayName("Terminate Method Tests")
  class TerminateTests {

    @Test
    @DisplayName("should call terminate on all commands in the DAG")
    void terminate_shouldCallTerminateOnAllCommands() throws Exception {
      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> node2 =
          Node.<OperatorCommand>builder().id(NODE_ID_2).nodeContent(mockScalarCmd2).build();
      Node<OperatorCommand> sinkNode =
          Node.<OperatorCommand>builder().id(SINK_ID).nodeContent(mockSinkCmd).build();
      List<Node<OperatorCommand>> nodes = List.of(node1, node2, sinkNode);
      List<Edge> edges =
          List.of( // Define some edges, structure doesn't matter for terminate
              Edge.builder().from(NODE_ID_1).to(NODE_ID_2).build(),
              Edge.builder().from(NODE_ID_2).to(SINK_ID).build());
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges); // Create the real DAG

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, false);

      noSourceDagRunner.terminate();

      verify(mockScalarCmd1).terminate();
      verify(mockScalarCmd2).terminate();
      verify(mockSinkCmd).terminate();
    }

    @Test
    @DisplayName("should handle exceptions during command termination")
    void terminate_shouldHandleExceptionsDuringTermination() throws Exception {
      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> node2 =
          Node.<OperatorCommand>builder().id(NODE_ID_2).nodeContent(mockScalarCmd2).build();
      List<Node<OperatorCommand>> nodes = List.of(node1, node2);
      List<Edge> edges = List.of(Edge.builder().from(NODE_ID_1).to(NODE_ID_2).build());
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges); // Create the real DAG

      doThrow(new RuntimeException("Terminate failed")).when(mockScalarCmd1).terminate();

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, false);

      assertDoesNotThrow(
          () -> noSourceDagRunner.terminate(), "Terminate should not throw exceptions upwards");
      verify(mockScalarCmd1).terminate(); // Called, but threw exception
      verify(mockScalarCmd2).terminate(); // Should still be called
    }
  }

  @Nested
  @DisplayName("DagRunConfig Output Tests")
  class DagRunConfigOutputTests {

    @Test
    @DisplayName("should include step output when configured")
    void run_shouldIncludeStepOutputWhenConfigured() {
      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> node2 =
          Node.<OperatorCommand>builder().id(NODE_ID_2).nodeContent(mockScalarCmd2).build();
      List<Node<OperatorCommand>> nodes = List.of(node1, node2);
      List<Edge> edges = List.of(Edge.builder().from(NODE_ID_1).to(NODE_ID_2).build());
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges);

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, false);

      DagResult result =
          noSourceDagRunner.run(
              inputEvents, CALLING_USER, runConfigIncludeAll); // includeOutputByStep=true

      assertFalse(result.outputByStep.isEmpty(), "outputByStep should not be empty");
      assertDebugInfo(result.outputByStep, NODE_ID_1, SOURCE_NODE_ID, inputEvents, "outputByStep");
      assertDebugInfo(result.outputByStep, NODE_ID_2, NODE_ID_1, inputEvents, "outputByStep");
      assertTrue(
          result.outputEvents.containsKey(NODE_ID_2), "outputEvents should contain final node ID");
      assertEquals(
          inputEvents,
          result.outputEvents.get(NODE_ID_2),
          "Final node outputEvents content mismatch");
    }

    @Test
    @DisplayName("should exclude step output when configured")
    void run_shouldExcludeStepOutputWhenConfigured() {

      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> node2 =
          Node.<OperatorCommand>builder().id(NODE_ID_2).nodeContent(mockScalarCmd2).build();
      List<Node<OperatorCommand>> nodes = List.of(node1, node2);
      List<Edge> edges = List.of(Edge.builder().from(NODE_ID_1).to(NODE_ID_2).build());
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges);

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, false);

      DagResult result = noSourceDagRunner.run(inputEvents, CALLING_USER, runConfigExcludeSteps);

      assertTrue(
          result.outputByStep.isEmpty(),
          "outputByStep map should be empty when includeOutputByStep=false");
      assertTrue(
          result.outputEvents.containsKey(NODE_ID_2),
          "outputEvents should still contain final node ID");
      assertEquals(
          inputEvents,
          result.outputEvents.get(NODE_ID_2),
          "Final node outputEvents content mismatch");
    }

    @Test
    @DisplayName("should include sink output in outputEvents even when step output is excluded")
    void run_shouldIncludeSinkOutputInOutputEventsWhenStepOutputIsExcluded() {

      Node<OperatorCommand> node1 =
          Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(mockScalarCmd1).build();
      Node<OperatorCommand> sinkNode =
          Node.<OperatorCommand>builder().id(SINK_ID).nodeContent(mockSinkCmd).build();
      List<Node<OperatorCommand>> nodes = List.of(node1, sinkNode);
      List<Edge> edges = List.of(Edge.builder().from(NODE_ID_1).to(SINK_ID).build());
      Dag<OperatorCommand> compiledDag = new Dag<>(nodes, edges);

      noSourceDagRunner =
          new NoSourceDagRunner(
              edgesFromSource, compiledDag, mockMetricProvider, mockCounters, false);

      DagResult result =
          noSourceDagRunner.run(
              inputEvents, CALLING_USER, runConfigExcludeSteps); // includeOutputByStep=false

      assertTrue(result.outputByStep.isEmpty(), "outputByStep map should be empty");
      assertTrue(result.outputEvents.containsKey(SINK_ID), "outputEvents should contain sink ID");
      var sinkResult =
          new ScalarSinkCommand.SinkResult(inputEvents.size(), inputEvents.size(), List.of());
      var outputEvent = sinkResultToOutputEvent(sinkResult);
      assertEquals(
          List.of(outputEvent),
          result.outputEvents.get(SINK_ID),
          "Sink outputEvents content mismatch");
    }
  }

  @Test
  public void test() {
    OperatorCommand command =
        OperatorCommandRegistry.OPERATOR_COMMANDS
            .get("eval")
            .createCommand(
                NODE_ID_1,
                JobContext.builder()
                    .metricTags(
                        Map.of(
                            METRIC_TAG_SERVICE, "my_service",
                            METRIC_TAG_ENV, "my_env"))
                    .build());
    command.parseAndValidateArg(Map.of("expression", "$.abc"));
    noSourceDagRunner =
        new NoSourceDagRunner(
            edgesFromSource,
            new Dag<>(
                List.of(Node.<OperatorCommand>builder().id(NODE_ID_1).nodeContent(command).build()),
                List.of()),
            new MetricClientProvider.NoopMetricClientProvider(),
            mockCounters,
            false);

    DagResult dagResult =
        noSourceDagRunner.run(
            List.of(((RecordFleakData) FleakData.wrap(Map.of()))),
            CALLING_USER,
            new NoSourceDagRunner.DagRunConfig(true, true));
    System.out.println(toJsonString(dagResult));

    assertTrue(dagResult.outputEvents.get(NODE_ID_1).isEmpty());
    assertTrue(dagResult.outputByStep.get(NODE_ID_1).get(SOURCE_NODE_ID).isEmpty());
    assertTrue(dagResult.errorByStep.isEmpty());
  }
}
