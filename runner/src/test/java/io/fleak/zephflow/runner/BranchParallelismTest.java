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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.NodeExecutionException;
import io.fleak.zephflow.runner.dag.Dag;
import io.fleak.zephflow.runner.dag.Edge;
import io.fleak.zephflow.runner.dag.Node;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class BranchParallelismTest {

  private static final String SOURCE = "source";
  private static final String CALLING_USER = "test-user";
  private static final NoSourceDagRunner.DagRunConfig RUN_CONFIG =
      new NoSourceDagRunner.DagRunConfig(true, true);

  private final MetricClientProvider metricProvider = mock(MetricClientProvider.class);
  private final List<RecordFleakData> events = List.of(event("e1"), event("e2"));

  private final Map<String, List<String>> threadsByNode = new ConcurrentHashMap<>();

  private interface SinkBody {
    int write(List<RecordFleakData> events) throws Exception;
  }

  @Test
  void groupBranches_independentSinksGetOneGroupEach() {
    Dag<String> dag = dag(List.of("a", "b", "c"), List.of());
    List<Edge> out = List.of(edge(SOURCE, "a"), edge(SOURCE, "b"), edge(SOURCE, "c"));

    List<List<Edge>> groups = NoSourceDagRunner.groupBranches(out, dag);

    assertEquals(List.of(List.of(out.get(0)), List.of(out.get(1)), List.of(out.get(2))), groups);
  }

  @Test
  void groupBranches_branchesReachingACommonNodeShareAGroup() {
    Dag<String> dag =
        dag(
            List.of("a", "x", "b", "c", "shared"),
            List.of(edge("a", "x"), edge("x", "shared"), edge("b", "shared")));
    List<Edge> out = List.of(edge(SOURCE, "a"), edge(SOURCE, "c"), edge(SOURCE, "b"));

    List<List<Edge>> groups = NoSourceDagRunner.groupBranches(out, dag);

    assertEquals(List.of(List.of(out.get(0), out.get(2)), List.of(out.get(1))), groups);
  }

  @Test
  void groupBranches_groupsMergedThroughAThirdBranch() {
    Dag<String> dag =
        dag(
            List.of("a", "b", "c", "s1", "s2"),
            List.of(edge("a", "s1"), edge("b", "s1"), edge("b", "s2"), edge("c", "s2")));
    List<Edge> out = List.of(edge(SOURCE, "a"), edge(SOURCE, "c"), edge(SOURCE, "b"));

    assertEquals(List.of(out), NoSourceDagRunner.groupBranches(out, dag));
  }

  @Test
  void independentSinksRunConcurrently() {
    CountDownLatch bothStarted = new CountDownLatch(2);
    SinkBody waitForSibling =
        batch -> {
          bothStarted.countDown();
          // sequential execution would never release the latch
          assertTrue(bothStarted.await(10, TimeUnit.SECONDS), "sibling sink did not run alongside");
          return batch.size();
        };
    Map<String, OperatorCommand> nodes = new LinkedHashMap<>();
    nodes.put("s1", sink("s1", waitForSibling));
    nodes.put("s2", sink("s2", waitForSibling));

    DagResult result =
        runner(fanOut("s1", "s2"), nodes, List.of(), mock(DagRunCounters.class), false, 4)
            .run(events, CALLING_USER, RUN_CONFIG);

    assertEquals(2, result.sinkResultMap.get("s1").getSuccessCount());
    assertEquals(2, result.sinkResultMap.get("s2").getSuccessCount());
    assertNotEquals(threadsByNode.get("s1"), threadsByNode.get("s2"));
  }

  @Test
  void parallelResultEqualsSequentialResult() {
    DagResult sequential = runMixedDag(1);
    DagResult parallel = runMixedDag(4);

    assertEquals(sequential.outputEvents, parallel.outputEvents);
    assertEquals(sequential.outputByStep, parallel.outputByStep);
    assertEquals(sequential.errorByStep, parallel.errorByStep);
    assertEquals(sequential.firstFailure, parallel.firstFailure);
  }

  @Test
  void parallelismOneRunsEverythingOnTheCallerThread() {
    runMixedDag(1);

    String caller = Thread.currentThread().getName();
    threadsByNode.forEach(
        (nodeId, threads) -> assertEquals(Set.of(caller), Set.copyOf(threads), nodeId));
  }

  @Test
  void convergentBranchesRunSequentiallyAndSharedSinkIsNeverConcurrent() {
    AtomicInteger active = new AtomicInteger();
    AtomicInteger maxActive = new AtomicInteger();
    Map<String, OperatorCommand> nodes = new LinkedHashMap<>();
    nodes.put("eval", passThrough("eval"));
    nodes.put("enrichment", passThrough("enrichment"));
    nodes.put(
        "s1",
        sink(
            "s1",
            batch -> {
              maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
              Thread.sleep(50);
              active.decrementAndGet();
              return batch.size();
            }));
    nodes.put("s2", sink("s2", List::size));
    List<Edge> edges =
        List.of(edge("eval", "s1"), edge("enrichment", "s1"), edge("enrichment", "s2"));

    DagResult result =
        runner(fanOut("eval", "enrichment"), nodes, edges, mock(DagRunCounters.class), false, 4)
            .run(events, CALLING_USER, RUN_CONFIG);

    assertEquals(1, maxActive.get(), "s1 was written by two threads at once");
    assertEquals(4, result.sinkResultMap.get("s1").getSuccessCount(), "s1 written once per path");
    assertEquals(threadsByNode.get("eval"), threadsByNode.get("enrichment"));
  }

  @Test
  void nestedFanOutInsideAParallelBranchRunsSequentially() {
    Map<String, OperatorCommand> nodes = new LinkedHashMap<>();
    nodes.put("a", passThrough("a"));
    nodes.put("b", sink("b", List::size));
    nodes.put("a1", sink("a1", List::size));
    nodes.put("a2", sink("a2", List::size));

    runner(
            fanOut("a", "b"),
            nodes,
            List.of(edge("a", "a1"), edge("a", "a2")),
            mock(DagRunCounters.class),
            false,
            4)
        .run(events, CALLING_USER, RUN_CONFIG);

    assertEquals(threadsByNode.get("a"), threadsByNode.get("a1"));
    assertEquals(threadsByNode.get("a"), threadsByNode.get("a2"));
  }

  @Test
  void failedBranchWaitsForSlowSiblingAndStillReachesTheDlq() {
    AtomicBoolean slowFinished = new AtomicBoolean();
    Map<String, OperatorCommand> nodes = new LinkedHashMap<>();
    nodes.put(
        "failing",
        sink(
            "failing",
            batch -> {
              throw new RuntimeException("boom");
            }));
    nodes.put(
        "slow",
        sink(
            "slow",
            batch -> {
              Thread.sleep(200);
              slowFinished.set(true);
              return batch.size();
            }));

    NoSourceDagRunner runner =
        runner(fanOut("failing", "slow"), nodes, List.of(), mock(DagRunCounters.class), true, 4);
    NodeExecutionException exception =
        assertThrows(
            NodeExecutionException.class, () -> runner.run(events, CALLING_USER, RUN_CONFIG));

    assertEquals("failing", exception.getNodeId());
    assertTrue(slowFinished.get(), "run() returned before the sibling branch finished");
  }

  @Test
  void unexpectedExceptionIsRethrownAfterAllBranchesFinish() {
    AtomicBoolean siblingFinished = new AtomicBoolean();
    OperatorCommand unsupported = mock(OperatorCommand.class);
    when(unsupported.commandName()).thenReturn("unsupported");
    Map<String, OperatorCommand> nodes = new LinkedHashMap<>();
    nodes.put("bad", unsupported);
    nodes.put(
        "slow",
        sink(
            "slow",
            batch -> {
              Thread.sleep(200);
              siblingFinished.set(true);
              return batch.size();
            }));

    NoSourceDagRunner runner =
        runner(fanOut("bad", "slow"), nodes, List.of(), mock(DagRunCounters.class), false, 4);

    assertThrows(IllegalStateException.class, () -> runner.run(events, CALLING_USER, RUN_CONFIG));
    assertTrue(siblingFinished.get());
  }

  @Test
  void pipelineCountersAreOnlyTouchedByTheCallerThread() {
    DagRunCounters counters = mock(DagRunCounters.class);
    Set<String> counterThreads = ConcurrentHashMap.newKeySet();
    doAnswer(invocation -> counterThreads.add(Thread.currentThread().getName()))
        .when(counters)
        .increaseOutputEventCounter(anyLong(), anyMap());
    doAnswer(invocation -> counterThreads.add(Thread.currentThread().getName()))
        .when(counters)
        .increaseErrorEventCounter(anyLong(), anyMap());
    Map<String, OperatorCommand> nodes = new LinkedHashMap<>();
    nodes.put("s1", sink("s1", List::size));
    nodes.put(
        "s2",
        sink(
            "s2",
            batch -> {
              throw new RuntimeException("boom");
            }));

    runner(fanOut("s1", "s2"), nodes, List.of(), counters, false, 4)
        .run(events, CALLING_USER, RUN_CONFIG);

    assertEquals(Set.of(Thread.currentThread().getName()), counterThreads);
    verify(counters).increaseOutputEventCounter(eq(2L), anyMap());
    verify(counters).increaseErrorEventCounter(eq(2L), anyMap());
  }

  @Test
  void mdcReachesWorkerThreads() {
    Map<String, String> mdcByNode = new ConcurrentHashMap<>();
    Map<String, OperatorCommand> nodes = new LinkedHashMap<>();
    for (String id : List.of("s1", "s2", "s3")) {
      nodes.put(
          id,
          sink(
              id,
              batch -> {
                mdcByNode.put(id, String.valueOf(MDC.get("callingUser")));
                return batch.size();
              }));
    }

    runner(fanOut("s1", "s2", "s3"), nodes, List.of(), mock(DagRunCounters.class), false, 4)
        .run(events, CALLING_USER, RUN_CONFIG);

    assertEquals(Map.of("s1", CALLING_USER, "s2", CALLING_USER, "s3", CALLING_USER), mdcByNode);
  }

  @Test
  void parallelismCapsConcurrentGroups() {
    AtomicInteger active = new AtomicInteger();
    AtomicInteger maxActive = new AtomicInteger();
    SinkBody tracked =
        batch -> {
          maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
          Thread.sleep(30);
          active.decrementAndGet();
          return batch.size();
        };
    Map<String, OperatorCommand> nodes = new LinkedHashMap<>();
    for (String id : List.of("s1", "s2", "s3", "s4", "s5")) {
      nodes.put(id, sink(id, tracked));
    }

    DagResult result =
        runner(
                fanOut("s1", "s2", "s3", "s4", "s5"),
                nodes,
                List.of(),
                mock(DagRunCounters.class),
                false,
                2)
            .run(events, CALLING_USER, RUN_CONFIG);

    assertTrue(maxActive.get() <= 2, "more than 2 groups ran at once: " + maxActive.get());
    assertEquals(5, result.sinkResultMap.size());
  }

  @Test
  void repeatedRunsLoseNothing() {
    Map<String, OperatorCommand> nodes = new LinkedHashMap<>();
    List<String> sinks = List.of("s1", "s2", "s3", "s4");
    for (String id : sinks) {
      nodes.put(id, sink(id, List::size));
    }
    NoSourceDagRunner runner =
        runner(
            fanOut(sinks.toArray(String[]::new)),
            nodes,
            List.of(),
            mock(DagRunCounters.class),
            false,
            4);

    for (int runIndex = 0; runIndex < 300; runIndex++) {
      DagResult result = runner.run(events, CALLING_USER, RUN_CONFIG);
      assertEquals(Set.copyOf(sinks), result.sinkResultMap.keySet());
      result
          .sinkResultMap
          .values()
          .forEach(sinkResult -> assertEquals(2, sinkResult.getSuccessCount()));
      assertEquals(Set.copyOf(sinks), result.outputEvents.keySet());
    }
  }

  @Test
  void branchParallelismIsReadFromTheJobContext() {
    assertEquals(1, NoSourceDagRunner.branchParallelism(null));
    assertEquals(1, NoSourceDagRunner.branchParallelism(jobContext(null)));
    assertEquals(4, NoSourceDagRunner.branchParallelism(jobContext(4)));
    assertEquals(4, NoSourceDagRunner.branchParallelism(jobContext(" 4 ")));
    assertEquals(1, NoSourceDagRunner.branchParallelism(jobContext("0")));
    assertEquals(1, NoSourceDagRunner.branchParallelism(jobContext("many")));
  }

  private DagResult runMixedDag(int parallelism) {
    threadsByNode.clear();
    Map<String, OperatorCommand> nodes = new LinkedHashMap<>();
    nodes.put("p1", passThrough("p1"));
    nodes.put("p2", passThrough("p2"));
    nodes.put("s1", sink("s1", List::size));
    nodes.put("s2", sink("s2", batch -> batch.size() - 1));
    nodes.put("s3", sink("s3", List::size));
    nodes.put("s4", sink("s4", List::size));
    List<Edge> edges = List.of(edge("p1", "s1"), edge("p1", "s2"), edge("p2", "s3"));
    return runner(
            fanOut("p1", "p2", "s4"), nodes, edges, mock(DagRunCounters.class), false, parallelism)
        .run(events, CALLING_USER, RUN_CONFIG);
  }

  private NoSourceDagRunner runner(
      List<Edge> edgesFromSource,
      Map<String, OperatorCommand> commands,
      List<Edge> edges,
      DagRunCounters counters,
      boolean useDlq,
      int parallelism) {
    List<Node<OperatorCommand>> nodes = new ArrayList<>();
    commands.forEach(
        (id, command) ->
            nodes.add(Node.<OperatorCommand>builder().id(id).nodeContent(command).build()));
    return new NoSourceDagRunner(
        edgesFromSource, new Dag<>(nodes, edges), metricProvider, counters, useDlq, parallelism);
  }

  private ScalarSinkCommand sink(String id, SinkBody body) {
    ScalarSinkCommand sink = mock(ScalarSinkCommand.class);
    when(sink.commandName()).thenReturn("sink-" + id);
    when(sink.getExecutionContext()).thenReturn(mock(ExecutionContext.class));
    when(sink.writeToSink(anyList(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              recordThread(id);
              List<RecordFleakData> batch = invocation.getArgument(0);
              int success = body.write(batch);
              List<ErrorOutput> errors = new ArrayList<>();
              for (int eventIndex = success; eventIndex < batch.size(); eventIndex++) {
                errors.add(new ErrorOutput(batch.get(eventIndex), "rejected by " + id));
              }
              return new ScalarSinkCommand.SinkResult(batch.size(), success, errors);
            });
    return sink;
  }

  private ScalarCommand passThrough(String id) {
    ScalarCommand command = mock(ScalarCommand.class);
    when(command.commandName()).thenReturn("pass-" + id);
    when(command.getExecutionContext()).thenReturn(mock(ExecutionContext.class));
    when(command.process(anyList(), anyString(), any()))
        .thenAnswer(
            invocation -> {
              recordThread(id);
              return new ScalarCommand.ProcessResult(invocation.getArgument(0), List.of());
            });
    return command;
  }

  private void recordThread(String nodeId) {
    threadsByNode
        .computeIfAbsent(nodeId, newNodeId -> Collections.synchronizedList(new ArrayList<>()))
        .add(Thread.currentThread().getName());
  }

  private static List<Edge> fanOut(String... targets) {
    return Arrays.stream(targets).map(target -> edge(SOURCE, target)).toList();
  }

  private static Edge edge(String from, String to) {
    return Edge.builder().from(from).to(to).build();
  }

  private static Dag<String> dag(List<String> ids, List<Edge> edges) {
    return new Dag<>(
        ids.stream().map(id -> Node.<String>builder().id(id).nodeContent(id).build()).toList(),
        edges);
  }

  private static JobContext jobContext(Serializable parallelism) {
    Map<String, Serializable> properties = new HashMap<>();
    if (parallelism != null) {
      properties.put(JobContext.BRANCH_PARALLELISM, parallelism);
    }
    return JobContext.builder().otherProperties(properties).build();
  }

  private static RecordFleakData event(String id) {
    return new RecordFleakData(Map.of("id", Objects.requireNonNull(FleakData.wrap(id))));
  }
}
