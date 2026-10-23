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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import io.fleak.zephflow.api.EndOfInputFlushable;
import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.KeyedStatefulCommand;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.WindowFlushable;
import io.fleak.zephflow.api.execution.*;
import io.fleak.zephflow.api.execution.ExecutionObserver.Invocation;
import io.fleak.zephflow.api.execution.ExecutionObserver.Phase;
import io.fleak.zephflow.api.execution.ExecutionObserver.Side;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.NodeExecutionException;
import io.fleak.zephflow.runner.dag.Dag;
import io.fleak.zephflow.runner.dag.Edge;
import io.fleak.zephflow.runner.dag.Node;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.MDC;

/**
 * Created by bolei on 3/4/25
 *
 * <p>Synchronous DFS over the source-less DAG: each {@link #run} walks the graph on the caller's
 * thread; with {@link JobContext#BRANCH_PARALLELISM} above 1, independent branches run in parallel.
 *
 * <p>Two independent, related mechanisms guard keyed reduction commands:
 *
 * <ul>
 *   <li>The {@link #pipelineLock} is taken by {@code run} whenever the DAG contains any {@link
 *       KeyedStatefulCommand} node (per-key reduction state, keyed by time OR count — e.g. a
 *       windowed aggregation, or an event-driven throttle). That state is NOT thread-safe, so the
 *       lock serializes {@code run} both against the flush thread and against any concurrent {@code
 *       run} caller (e.g. a future multi-threaded source). Stateless pipelines take no lock, so
 *       they are unaffected.
 *   <li>The background flush scheduler (see {@link #startFlushScheduler}) is started only when the
 *       DAG contains a {@link WindowFlushable} node — the narrower subset that must fire a
 *       time-triggered window even when no input arrives. Count-only keyed commands (e.g. throttle)
 *       are stateful but not flushable, so they take the lock but spawn no flush thread.
 * </ul>
 *
 * <p>At end of input, {@link EndOfInputFlushable} nodes emit their pending output in topological
 * order, so an upstream node's flushed records reach a downstream stateful node before that node
 * flushes. A bounded caller (test run, inspection) declares end of input via {@link #run(List,
 * String, DagRunConfig, boolean)} and gets the output in the returned {@link DagResult}; {@link
 * #terminate} does the same for a finishing or shutting-down pipeline, routing to still-open sinks.
 */
@Slf4j
public class NoSourceDagRunner {

  private static final long DEFAULT_FLUSH_TICK_MS = 1000L;
  private static final DagRunConfig FLUSH_RUN_CONFIG = new DagRunConfig(false, false);

  // cached, not fixed: branch tasks mostly block on sink I/O
  private static final AtomicInteger BRANCH_THREAD_COUNT = new AtomicInteger();
  private static final ExecutorService BRANCH_POOL =
      Executors.newCachedThreadPool(
          runnable -> {
            Thread thread =
                new Thread(
                    runnable, "zephflow-dag-branch-" + BRANCH_THREAD_COUNT.incrementAndGet());
            thread.setDaemon(true);
            return thread;
          });

  @NonNull private final List<Edge> edgesFromSource;
  private final Dag<OperatorCommand> compiledDagWithoutSource;
  private final MetricClientProvider metricClientProvider;
  private final DagRunCounters counters;
  private final boolean useDlq;
  private final BoundedDefinition boundedDefinition;
  private BoundedExecution boundedExecution;

  private final List<Node<OperatorCommand>> windowedNodes;
  private final boolean hasWindowedNodes;
  private final List<Node<OperatorCommand>> endOfInputNodes;
  private final Map<String, String> endOfInputUpstreams;
  private final boolean hasKeyedStatefulNodes;
  private final ReentrantLock pipelineLock = new ReentrantLock();
  private final int branchParallelism;
  private final Map<String, List<List<Edge>>> branchGroups;
  private final AtomicBoolean terminated = new AtomicBoolean(false);

  private volatile ScheduledExecutorService flushScheduler;
  private volatile ScheduledFuture<?> flushTask;
  private volatile String flushCallingUser;

  public NoSourceDagRunner(
      @NonNull List<Edge> edgesFromSource,
      Dag<OperatorCommand> compiledDagWithoutSource,
      MetricClientProvider metricClientProvider,
      DagRunCounters counters,
      boolean useDlq) {
    this(edgesFromSource, compiledDagWithoutSource, metricClientProvider, counters, useDlq, 1);
  }

  NoSourceDagRunner(
      List<Edge> edgesFromSource,
      Dag<OperatorCommand> compiledDagWithoutSource,
      MetricClientProvider metricClientProvider,
      DagRunCounters counters,
      boolean useDlq,
      int branchParallelism) {
    this(
        edgesFromSource,
        compiledDagWithoutSource,
        metricClientProvider,
        counters,
        useDlq,
        null,
        branchParallelism);
  }

  NoSourceDagRunner(
      List<Edge> edgesFromSource,
      Dag<OperatorCommand> compiledDagWithoutSource,
      MetricClientProvider metricClientProvider,
      DagRunCounters counters,
      boolean useDlq,
      BoundedDefinition boundedDefinition) {
    this(
        edgesFromSource,
        compiledDagWithoutSource,
        metricClientProvider,
        counters,
        useDlq,
        boundedDefinition,
        1);
  }

  private NoSourceDagRunner(
      List<Edge> edgesFromSource,
      Dag<OperatorCommand> compiledDagWithoutSource,
      MetricClientProvider metricClientProvider,
      DagRunCounters counters,
      boolean useDlq,
      BoundedDefinition boundedDefinition,
      int branchParallelism) {
    this.edgesFromSource = edgesFromSource;
    this.compiledDagWithoutSource = compiledDagWithoutSource;
    this.metricClientProvider = metricClientProvider;
    this.counters = counters;
    this.useDlq = useDlq;
    this.boundedDefinition = boundedDefinition;
    this.windowedNodes =
        compiledDagWithoutSource.getNodes().stream()
            .filter(n -> n.getNodeContent() instanceof WindowFlushable)
            .toList();
    this.hasWindowedNodes = !windowedNodes.isEmpty();
    this.hasKeyedStatefulNodes =
        compiledDagWithoutSource.getNodes().stream()
            .anyMatch(n -> n.getNodeContent() instanceof KeyedStatefulCommand);
    this.endOfInputNodes =
        topologicalOrder(compiledDagWithoutSource).stream()
            .filter(n -> n.getNodeContent() instanceof EndOfInputFlushable)
            .toList();
    this.endOfInputUpstreams = new HashMap<>();
    for (Node<OperatorCommand> node : endOfInputNodes) {
      endOfInputUpstreams.put(node.getId(), firstUpstream(node.getId()));
    }
    this.branchParallelism = Math.max(1, branchParallelism);
    this.branchGroups =
        this.branchParallelism > 1 && boundedDefinition == null ? computeBranchGroups() : Map.of();
    if (!branchGroups.isEmpty()) {
      log.info(
          "branch parallelism {}: parallel branches after fan-out nodes {}",
          this.branchParallelism,
          branchGroups.keySet());
    }
  }

  static int branchParallelism(JobContext jobContext) {
    Object value =
        jobContext == null || jobContext.getOtherProperties() == null
            ? null
            : jobContext.getOtherProperties().get(JobContext.BRANCH_PARALLELISM);
    if (value == null) {
      return 1;
    }
    try {
      int parallelism = Integer.parseInt(value.toString().trim());
      if (parallelism >= 1) {
        return parallelism;
      }
    } catch (NumberFormatException ignored) {
    }
    log.warn("invalid {}={}; running branches sequentially", JobContext.BRANCH_PARALLELISM, value);
    return 1;
  }

  private Map<String, List<List<Edge>>> computeBranchGroups() {
    Map<String, List<Edge>> outgoingEdgesByNodeId = new LinkedHashMap<>();
    for (Edge edge : edgesFromSource) {
      outgoingEdgesByNodeId.computeIfAbsent(edge.getFrom(), nodeId -> new ArrayList<>()).add(edge);
    }
    for (Node<OperatorCommand> node : compiledDagWithoutSource.getNodes()) {
      outgoingEdgesByNodeId.put(
          node.getId(), compiledDagWithoutSource.downstreamEdges(node.getId()));
    }
    Map<String, List<List<Edge>>> groupsByFanOutNodeId = new HashMap<>();
    outgoingEdgesByNodeId.forEach(
        (nodeId, outgoingEdges) -> {
          if (outgoingEdges.size() < 2) {
            return;
          }
          List<List<Edge>> groups = groupBranches(outgoingEdges, compiledDagWithoutSource);
          if (groups.size() > 1) {
            groupsByFanOutNodeId.put(nodeId, groups);
          }
        });
    return Map.copyOf(groupsByFanOutNodeId);
  }

  static List<List<Edge>> groupBranches(List<Edge> outgoingEdges, Dag<?> dag) {
    int edgeCount = outgoingEdges.size();
    List<Set<String>> reachableNodeIdsByEdge = new ArrayList<>();
    for (Edge edge : outgoingEdges) {
      reachableNodeIdsByEdge.add(reachableNodeIds(edge.getTo(), dag));
    }
    int[] parentEdgeIndex = new int[edgeCount];
    for (int edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++) {
      parentEdgeIndex[edgeIndex] = edgeIndex;
    }
    for (int edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++) {
      for (int otherEdgeIndex = edgeIndex + 1; otherEdgeIndex < edgeCount; otherEdgeIndex++) {
        if (!Collections.disjoint(
            reachableNodeIdsByEdge.get(edgeIndex), reachableNodeIdsByEdge.get(otherEdgeIndex))) {
          parentEdgeIndex[findGroupRoot(parentEdgeIndex, otherEdgeIndex)] =
              findGroupRoot(parentEdgeIndex, edgeIndex);
        }
      }
    }
    Map<Integer, List<Edge>> edgesByGroupRoot = new LinkedHashMap<>();
    for (int edgeIndex = 0; edgeIndex < edgeCount; edgeIndex++) {
      edgesByGroupRoot
          .computeIfAbsent(
              findGroupRoot(parentEdgeIndex, edgeIndex), groupRoot -> new ArrayList<>())
          .add(outgoingEdges.get(edgeIndex));
    }
    return List.copyOf(edgesByGroupRoot.values());
  }

  private static int findGroupRoot(int[] parentEdgeIndex, int edgeIndex) {
    while (parentEdgeIndex[edgeIndex] != edgeIndex) {
      edgeIndex = parentEdgeIndex[edgeIndex];
    }
    return edgeIndex;
  }

  private static Set<String> reachableNodeIds(String startNodeId, Dag<?> dag) {
    Set<String> reachedNodeIds = new HashSet<>();
    Deque<String> nodeIdsToVisit = new ArrayDeque<>(List.of(startNodeId));
    while (!nodeIdsToVisit.isEmpty()) {
      String nodeId = nodeIdsToVisit.pop();
      if (reachedNodeIds.add(nodeId)) {
        dag.downstreamEdges(nodeId).forEach(edge -> nodeIdsToVisit.push(edge.getTo()));
      }
    }
    return reachedNodeIds;
  }

  public DagResult run(
      List<RecordFleakData> events, String callingUser, NoSourceDagRunner.DagRunConfig runConfig) {
    return run(events, callingUser, runConfig, false);
  }

  /**
   * Runs each complete binding once, in authored node order, then flushes stateful operators once.
   * The caller must subsequently call disposeBounded, including when this method throws.
   */
  public BoundedRunResult runBounded(
      List<BoundInput> inputs,
      String callingUser,
      ExecutionObserver observer,
      ExecutionControl control) {
    pipelineLock.lock();
    try {
      Preconditions.checkState(
          boundedDefinition != null, "Runner was not prepared for bounded execution");
      Preconditions.checkState(
          boundedExecution == null && !terminated.get(), "Bounded runner is single-use");
      Map<String, BoundInput> bindings = validateBindings(inputs);
      boundedExecution = new BoundedExecution(boundedDefinition, observer, control);
      for (var node : compiledDagWithoutSource.getNodes()) {
        if (!boundedDefinition.boundaries().containsKey(node.getId())) {
          node.getNodeContent().setExecutionHooks(boundedExecution.hooks(node.getId()));
        }
      }
      for (var node : compiledDagWithoutSource.getNodes()) {
        if (!boundedDefinition.boundaries().containsKey(node.getId())) {
          initializeBoundedCommand(node.getId(), node.getNodeContent(), boundedExecution);
        }
      }
      RunContext context =
          RunContext.builder()
              .callingUser(callingUser)
              .callingUserTag(getCallingUserTagAndEventTags(callingUser, null))
              .metricClientProvider(metricClientProvider)
              .runConfig(FLUSH_RUN_CONFIG)
              .bounded(boundedExecution)
              .build();
      for (var node : compiledDagWithoutSource.getNodes()) {
        BoundInput input = bindings.get(node.getId());
        if (input == null) continue;
        boundedExecution.checkpoint();
        context.bindingId = input.bindingId();
        if (input.side() == Side.OUTPUT) {
          Invocation invocation =
              boundedExecution.start(input.nodeId(), null, Phase.PROCESS, input.bindingId());
          boundedExecution.records(invocation, Side.OUTPUT, input.records());
          boundedExecution.finish(invocation, 0, input.records().size(), true);
          routeToDownstream(
              input.nodeId(),
              node.getNodeContent().commandName(),
              input.records(),
              compiledDagWithoutSource.downstreamEdges(input.nodeId()),
              context);
        } else {
          processEvent(input.nodeId(), null, input.records(), context);
        }
      }
      context.bindingId = null;
      flushEndOfInputNodes(context);
      boundedExecution.checkpoint();
      return boundedExecution.summary();
    } finally {
      pipelineLock.unlock();
    }
  }

  private Map<String, BoundInput> validateBindings(List<BoundInput> inputs) {
    Map<String, BoundInput> bindings = new HashMap<>();
    for (BoundInput input : inputs) {
      compiledDagWithoutSource.lookupNode(input.nodeId());
      Preconditions.checkArgument(
          bindings.putIfAbsent(input.nodeId(), input) == null,
          "Duplicate node binding: %s",
          input.nodeId());
      Preconditions.checkArgument(
          compiledDagWithoutSource.upstreamEdges(input.nodeId()).isEmpty(),
          "Bound input must replace all upstream edges: %s",
          input.nodeId());
      boolean source =
          boundedDefinition.boundaries().get(input.nodeId())
              == BoundedDefinition.BoundaryRole.SOURCE_OUTPUT;
      Preconditions.checkArgument(
          source == (input.side() == Side.OUTPUT),
          "Only a source boundary accepts OUTPUT binding: %s",
          input.nodeId());
    }
    for (var entry : compiledDagWithoutSource.getEntryNodes()) {
      Preconditions.checkArgument(
          bindings.containsKey(entry.getId()), "Missing entry binding: %s", entry.getId());
    }
    return bindings;
  }

  /** Releases resources without repeating end-of-input; abort never flushes pending user data. */
  public void disposeBounded(CompletionDisposition disposition) {
    Preconditions.checkState(
        boundedDefinition != null, "Runner was not prepared for bounded execution");
    pipelineLock.lock();
    try {
      if (!terminated.compareAndSet(false, true)) return;
      RuntimeException observationFailure = null;
      for (var node : compiledDagWithoutSource.getNodes()) {
        OperatorCommand command = node.getNodeContent();
        if (!command.isInitialized()) continue;
        Invocation invocation = null;
        Invocation previous = null;
        try {
          if (boundedExecution != null) {
            invocation = boundedExecution.start(node.getId(), null, Phase.DISPOSE, null);
            previous = boundedExecution.enter(invocation);
          }
        } catch (RuntimeException failure) {
          observationFailure = failure;
        }
        try {
          if (disposition == CompletionDisposition.ABORTED) command.abort();
          else command.terminate();
          if (invocation != null) boundedExecution.finish(invocation, 0, 0, true);
        } catch (Exception failure) {
          if (invocation != null) {
            try {
              boundedExecution.failed(invocation, failure);
              boundedExecution.finish(invocation, 0, 0, false);
            } catch (RuntimeException publicationFailure) {
              publicationFailure.addSuppressed(failure);
              observationFailure = publicationFailure;
            }
          } else if (observationFailure != null) {
            observationFailure.addSuppressed(failure);
          } else {
            observationFailure = new IllegalStateException("Bounded cleanup failed", failure);
          }
        } finally {
          if (boundedExecution != null) boundedExecution.restore(previous);
        }
      }
      if (observationFailure != null) throw observationFailure;
    } finally {
      pipelineLock.unlock();
    }
  }

  /**
   * Runs {@code events} through the DAG. With {@code endOfInput}, the input is declared finished
   * after these events: every {@link EndOfInputFlushable} node then emits its pending output into
   * the returned result.
   */
  public DagResult run(
      List<RecordFleakData> events,
      String callingUser,
      NoSourceDagRunner.DagRunConfig runConfig,
      boolean endOfInput) {
    return lockedRun(events, callingUser, runConfig, true, endOfInput);
  }

  /**
   * Declares end of input without new events, for a caller that fed its input over several {@link
   * #run} calls: only the {@link EndOfInputFlushable} nodes' pending output is emitted.
   */
  public DagResult finishInput(String callingUser, NoSourceDagRunner.DagRunConfig runConfig) {
    if (endOfInputNodes.stream().noneMatch(n -> n.getNodeContent().isInitialized())) {
      return new DagResult();
    }
    return lockedRun(List.of(), callingUser, runConfig, false, true);
  }

  private DagResult lockedRun(
      List<RecordFleakData> events,
      String callingUser,
      NoSourceDagRunner.DagRunConfig runConfig,
      boolean routeInput,
      boolean endOfInput) {
    Preconditions.checkState(boundedDefinition == null, "Use runBounded for this runner");
    // Any keyed-stateful node (windowed aggregation, throttle, sample) holds non-thread-safe
    // per-key
    // state, so take the lock to serialize run() against the flush thread and any concurrent run()
    // caller. Stateless pipelines skip the lock entirely (zero overhead, single-threaded).
    if (!hasKeyedStatefulNodes) {
      return doRun(events, callingUser, runConfig, routeInput, endOfInput);
    }
    pipelineLock.lock();
    try {
      return doRun(events, callingUser, runConfig, routeInput, endOfInput);
    } finally {
      pipelineLock.unlock();
    }
  }

  private DagResult doRun(
      List<RecordFleakData> events,
      String callingUser,
      NoSourceDagRunner.DagRunConfig runConfig,
      boolean routeInput,
      boolean endOfInput) {

    // Initialize all commands once at the start of the run
    initializeAllCommands();
    flushCallingUser = callingUser;

    // make sure all edges are from the same source
    var sourceNodeIds = edgesFromSource.stream().map(Edge::getFrom).distinct().toList();
    Preconditions.checkArgument(
        sourceNodeIds.size() == 1,
        String.format(
            "Only single source DAG is supported but found %d sources", sourceNodeIds.size()));
    var sourceNodeId = sourceNodeIds.getFirst();
    String commandName = "source_node";

    Map<String, String> tags =
        getCallingUserTagAndEventTags(callingUser, events.isEmpty() ? null : events.getFirst());

    counters.increaseInputEventCounter(events.size(), tags);
    counters.startStopWatch();
    MDC.put("callingUser", callingUser);
    log.debug("events {}", events.size());

    DagResult dagResult = new DagResult();
    RunContext runContext =
        RunContext.builder()
            .callingUser(callingUser)
            .callingUserTag(tags)
            .dagResult(dagResult)
            .metricClientProvider(metricClientProvider)
            .runConfig(runConfig)
            .build();
    if (routeInput) {
      routeToDownstream(sourceNodeId, commandName, events, edgesFromSource, runContext);
    }
    if (endOfInput) {
      flushEndOfInputNodes(runContext);
    }
    counters.stopStopWatch(tags);
    MDC.clear();
    dagResult.consolidateSinkResult(); // merge all sinkResults and put them into outputEvents
    if (log.isDebugEnabled() && MapUtils.isNotEmpty(dagResult.getErrorByStep())) {
      log.debug("failed to process events: {}", toJsonString(dagResult.errorByStep));
    }
    // Per-node failures are isolated during the traversal (siblings still run). When DLQ is enabled
    // we surface them here, after every branch has run, so the source persists the whole raw record
    // to the DLQ (at-least-once). Without DLQ the failures are counted and dropped (at-most-once).
    if (useDlq && dagResult.hasFailure()) {
      DagResult.NodeFailure failure = dagResult.getFirstFailure();
      throw new NodeExecutionException(
          failure.nodeId(),
          failure.commandName(),
          failure.errorMessage(),
          new IllegalArgumentException(failure.errorMessage()));
    }
    return dagResult;
  }

  void routeToDownstream(
      String currentNodeId,
      String commandName,
      List<RecordFleakData> events,
      List<Edge> outgoingEdges,
      RunContext runContext) {
    if (CollectionUtils.isEmpty(outgoingEdges)) {
      if (runContext.bounded != null) return;
      List<RecordFleakData> currentNodeOutput =
          runContext.dagResult.outputEvents.computeIfAbsent(currentNodeId, k -> new ArrayList<>());
      currentNodeOutput.addAll(events);
      Map<String, String> tags = new HashMap<>(runContext.callingUserTag);
      tags.put(METRIC_TAG_NODE_ID, currentNodeId);
      tags.put(METRIC_TAG_COMMAND_NAME, commandName);
      countersFor(runContext).increaseOutputEventCounter(events.size(), tags);
      return;
    }
    List<List<Edge>> groups =
        runContext.bounded != null || runContext.inParallelBranch
            ? null
            : branchGroups.get(currentNodeId);
    if (groups == null) {
      routeSequentially(currentNodeId, events, outgoingEdges, runContext);
    } else {
      routeInParallel(currentNodeId, events, outgoingEdges, groups, runContext);
    }
  }

  private void routeSequentially(
      String currentNodeId,
      List<RecordFleakData> events,
      List<Edge> outgoingEdges,
      RunContext runContext) {
    for (var e : outgoingEdges) {
      if (runContext.bounded != null) runContext.bounded.checkpoint();
      // Failure isolation: a node failure aborts only its own subtree; sibling branches still run.
      // Recorded failures may trigger the DLQ once the whole run finishes (see run()).
      // Data isolation relies on the command contract (no in-place mutation of input records), so
      // fan-out branches safely share the same event objects with no per-branch copy.
      try {
        processEvent(e.getTo(), currentNodeId, events, runContext);
      } catch (NodeExecutionException nee) {
        if (runContext.bounded != null) throw nee;
        runContext.dagResult.recordFailure(nee.getNodeId(), nee.getCommandName(), nee.getMessage());
        Map<String, String> tags = new HashMap<>(runContext.callingUserTag);
        tags.put(METRIC_TAG_NODE_ID, nee.getNodeId());
        tags.put(METRIC_TAG_COMMAND_NAME, nee.getCommandName());
        countersFor(runContext).increaseErrorEventCounter(events.size(), tags);
        log.debug("node {} failed; isolating branch", nee.getNodeId(), nee);
      }
    }
  }

  private void routeInParallel(
      String currentNodeId,
      List<RecordFleakData> events,
      List<Edge> outgoingEdges,
      List<List<Edge>> groups,
      RunContext runContext) {
    List<BranchRun> branchRuns = new ArrayList<>();
    for (List<Edge> group : groups) {
      List<Integer> edgeIndexes = group.stream().map(outgoingEdges::indexOf).toList();
      branchRuns.add(new BranchRun(group, edgeIndexes, runContext));
    }
    int laneCount = Math.min(branchRuns.size(), branchParallelism);
    Map<String, String> callerMdcContext = MDC.getCopyOfContextMap();
    List<Future<?>> submittedLanes = new ArrayList<>();
    for (int laneIndex = 1; laneIndex < laneCount; laneIndex++) {
      List<BranchRun> laneBranchRuns = branchRunsForLane(branchRuns, laneIndex, laneCount);
      try {
        submittedLanes.add(
            BRANCH_POOL.submit(
                () ->
                    runLaneOnPoolThread(currentNodeId, events, laneBranchRuns, callerMdcContext)));
      } catch (RejectedExecutionException | OutOfMemoryError submitFailure) {
        log.warn("could not start a parallel branch lane; running it inline", submitFailure);
        runLane(currentNodeId, events, laneBranchRuns);
      }
    }
    runLane(currentNodeId, events, branchRunsForLane(branchRuns, 0, laneCount));

    // wait for every lane even after a failure, or the source commits too early
    Throwable firstFailure = null;
    for (Future<?> submittedLane : submittedLanes) {
      try {
        Uninterruptibles.getUninterruptibly(submittedLane);
      } catch (ExecutionException executionException) {
        firstFailure = addFailure(firstFailure, executionException.getCause());
      }
    }
    // merge in the order a sequential run would have hit the failures, so the same one goes first
    List<BranchRun> branchRunsInFailureOrder =
        branchRuns.stream()
            .sorted(Comparator.comparingInt(branchRun -> branchRun.firstFailureEdgeIndex))
            .toList();
    for (BranchRun branchRun : branchRunsInFailureOrder) {
      runContext.dagResult.merge(branchRun.context.dagResult);
      branchRun.counterDeltas.applyTo(countersFor(runContext));
      if (branchRun.failure != null) {
        firstFailure = addFailure(firstFailure, branchRun.failure);
      }
    }
    if (firstFailure instanceof RuntimeException runtimeException) throw runtimeException;
    if (firstFailure instanceof Error error) throw error;
    if (firstFailure != null) throw new IllegalStateException(firstFailure);
  }

  private static Throwable addFailure(Throwable firstFailure, Throwable failure) {
    if (firstFailure == null) return failure;
    firstFailure.addSuppressed(failure);
    return firstFailure;
  }

  private static List<BranchRun> branchRunsForLane(
      List<BranchRun> branchRuns, int laneIndex, int laneCount) {
    List<BranchRun> laneBranchRuns = new ArrayList<>();
    for (int groupIndex = laneIndex; groupIndex < branchRuns.size(); groupIndex += laneCount) {
      laneBranchRuns.add(branchRuns.get(groupIndex));
    }
    return laneBranchRuns;
  }

  private void runLaneOnPoolThread(
      String currentNodeId,
      List<RecordFleakData> events,
      List<BranchRun> laneBranchRuns,
      Map<String, String> callerMdcContext) {
    if (callerMdcContext == null) MDC.clear();
    else MDC.setContextMap(callerMdcContext);
    try {
      runLane(currentNodeId, events, laneBranchRuns);
    } finally {
      MDC.clear();
    }
  }

  private void runLane(
      String currentNodeId, List<RecordFleakData> events, List<BranchRun> laneBranchRuns) {
    for (BranchRun branchRun : laneBranchRuns) {
      for (int edgePosition = 0; edgePosition < branchRun.edges.size(); edgePosition++) {
        int edgeIndex = branchRun.edgeIndexes.get(edgePosition);
        try {
          routeSequentially(
              currentNodeId, events, List.of(branchRun.edges.get(edgePosition)), branchRun.context);
        } catch (Throwable failure) {
          branchRun.failure = failure;
          branchRun.recordFailureAt(edgeIndex);
          break;
        }
        if (branchRun.context.dagResult.hasFailure()) {
          branchRun.recordFailureAt(edgeIndex);
        }
      }
    }
  }

  private DagRunCounters countersFor(RunContext runContext) {
    return runContext.counterDeltas != null ? runContext.counterDeltas.recorder : counters;
  }

  void processEvent(
      String currentNodeId,
      String upstreamNodeId,
      List<RecordFleakData> events,
      RunContext runContext) {
    if (runContext.bounded != null) {
      processBoundedEvent(currentNodeId, upstreamNodeId, events, runContext);
      return;
    }
    Node<OperatorCommand> compiledNode = compiledDagWithoutSource.lookupNode(currentNodeId);
    OperatorCommand command = compiledNode.getNodeContent();
    List<Edge> downstreamEdges = compiledDagWithoutSource.downstreamEdges(currentNodeId);

    // Get the already-initialized execution context
    ExecutionContext executionContext = command.getExecutionContext();

    try {
      if (command instanceof ScalarCommand scalarCommand) {
        // Process the event through a scalar command
        ScalarCommand.ProcessResult result =
            scalarCommand.process(events, runContext.callingUser, executionContext);
        runContext.dagResult.handleNodeResult(
            runContext.callingUserTag,
            currentNodeId,
            upstreamNodeId,
            command.commandName(),
            runContext.runConfig,
            result.getOutput(),
            result.getFailureEvents(),
            countersFor(runContext));

        routeToDownstream(
            currentNodeId, command.commandName(), result.getOutput(), downstreamEdges, runContext);
        return;
      }
      if (command instanceof ScalarSinkCommand sinkCommand) {
        // Write to sink
        ScalarSinkCommand.SinkResult result =
            sinkCommand.writeToSink(events, runContext.callingUser, executionContext);
        RecordFleakData sinkOutputEvent = sinkResultToOutputEvent(result);
        runContext.dagResult.handleNodeResult(
            runContext.callingUserTag,
            currentNodeId,
            upstreamNodeId,
            command.commandName(),
            runContext.runConfig,
            List.of(sinkOutputEvent),
            result.getFailureEvents(),
            countersFor(runContext));
        Map<String, String> tags = new HashMap<>(runContext.callingUserTag);
        tags.put(METRIC_TAG_NODE_ID, currentNodeId);
        tags.put(METRIC_TAG_COMMAND_NAME, command.commandName());
        countersFor(runContext).increaseOutputEventCounter(result.getSuccessCount(), tags);

        if (runContext.dagResult.sinkResultMap.containsKey(currentNodeId)) {
          result.merge(runContext.dagResult.sinkResultMap.get(currentNodeId));
        }
        runContext.dagResult.sinkResultMap.put(currentNodeId, result);
        return;
      }
    } catch (NodeExecutionException e) {
      throw e;
    } catch (Exception e) {
      throw new NodeExecutionException(currentNodeId, command.commandName(), e.getMessage(), e);
    }
    throw new IllegalStateException(
        String.format(
            "encountered unsupported command at downstream node: id=%s, commandName=%s",
            currentNodeId, command.commandName()));
  }

  private void processBoundedEvent(
      String nodeId, String upstreamId, List<RecordFleakData> events, RunContext context) {
    BoundedExecution execution = context.bounded;
    execution.checkpoint();
    Invocation invocation =
        execution.start(
            nodeId, upstreamId, Phase.PROCESS, upstreamId == null ? context.bindingId : null);
    execution.records(invocation, Side.INPUT, events);
    var boundary = boundedDefinition.boundaries().get(nodeId);
    if (boundary != null) {
      Preconditions.checkState(
          boundary != BoundedDefinition.BoundaryRole.SOURCE_OUTPUT,
          "Source boundary cannot receive upstream records");
      execution.finish(
          invocation, events.size(), 0, boundary == BoundedDefinition.BoundaryRole.INPUT_ONLY);
      return;
    }
    OperatorCommand command = compiledDagWithoutSource.lookupNode(nodeId).getNodeContent();
    Invocation previous = execution.enter(invocation);
    List<RecordFleakData> output = null;
    try {
      execution.checkpoint();
      if (command instanceof ScalarCommand scalar) {
        Long effect =
            boundedDefinition.externalNodeIds().contains(nodeId)
                ? execution.hooks(nodeId).effects().started()
                : null;
        ScalarCommand.ProcessResult result = null;
        try {
          result = scalar.process(events, context.callingUser, command.getExecutionContext());
        } finally {
          if (effect != null)
            execution
                .hooks(nodeId)
                .effects()
                .finished(
                    effect,
                    EffectOutcome.opaque(
                        result != null
                            && result.getInvocationFailure() == null
                            && result.getFailureEvents().isEmpty()));
        }
        execution.errors(invocation, result.getFailureEvents());
        output = result.getOutput();
        execution.records(invocation, Side.OUTPUT, output);
        if (result.getInvocationFailure() != null) {
          execution.failed(invocation, result.getInvocationFailure());
          execution.finish(invocation, events.size(), output.size(), false);
          return;
        }
        execution.finish(
            invocation, events.size(), output.size(), result.getFailureEvents().isEmpty());
      } else if (command instanceof ScalarSinkCommand sink) {
        ScalarSinkCommand.SinkResult result =
            sink.writeToSink(events, context.callingUser, command.getExecutionContext());
        execution.errors(invocation, result.getFailureEvents());
        if (result.getInvocationFailure() != null)
          execution.failed(invocation, result.getInvocationFailure());
        execution.finish(
            invocation,
            events.size(),
            0,
            result.getFailureEvents().isEmpty() && result.getInvocationFailure() == null);
      } else {
        throw new IllegalStateException("Unsupported bounded operator: " + command.commandName());
      }
    } catch (ExecutionProgressStoppedException partial) {
      execution.errors(invocation, partial.errors());
      if (command instanceof ScalarCommand) {
        execution.records(invocation, Side.OUTPUT, partial.output());
      }
      execution.finish(invocation, events.size(), partial.output().size(), false);
      throw partial;
    } catch (ExecutionStoppedException failure) {
      throw failure;
    } catch (Exception failure) {
      execution.failed(invocation, failure);
      execution.finish(invocation, events.size(), 0, false);
      return;
    } finally {
      execution.restore(previous);
    }
    execution.checkpoint();
    if (output != null)
      routeToDownstream(
          nodeId,
          command.commandName(),
          output,
          compiledDagWithoutSource.downstreamEdges(nodeId),
          context);
  }

  private void initializeBoundedCommand(
      String nodeId, OperatorCommand command, BoundedExecution execution) {
    if (command.isInitialized()) return;
    execution.checkpoint();
    Invocation invocation = execution.start(nodeId, null, Phase.INIT, null);
    Invocation previous = execution.enter(invocation);
    try {
      Long effect =
          boundedDefinition.externalNodeIds().contains(nodeId)
              ? execution.hooks(nodeId).effects().started()
              : null;
      boolean returnedSuccessfully = false;
      try {
        command.initialize(metricClientProvider);
        returnedSuccessfully = true;
      } finally {
        if (effect != null)
          execution
              .hooks(nodeId)
              .effects()
              .finished(effect, EffectOutcome.opaque(returnedSuccessfully));
      }
      execution.checkpoint();
      execution.finish(invocation, 0, 0, true);
    } catch (ExecutionStoppedException failure) {
      throw failure;
    } catch (Exception failure) {
      execution.failed(invocation, failure);
      execution.finish(invocation, 0, 0, false);
      throw new ExecutionStoppedException("Operator initialization failed: " + nodeId, failure);
    } finally {
      execution.restore(previous);
    }
  }

  /**
   * Initialize all commands in the DAG. Should be called once before processing events. This method
   * is idempotent - calling it multiple times will only initialize each command once due to
   * double-checked locking in OperatorCommand.initialize().
   */
  private void initializeAllCommands() {
    for (Node<OperatorCommand> node : compiledDagWithoutSource.getNodes()) {
      OperatorCommand command = node.getNodeContent();
      try {
        command.initialize(metricClientProvider);
      } catch (NodeExecutionException e) {
        throw e;
      } catch (Exception e) {
        throw new NodeExecutionException(node.getId(), command.commandName(), e.getMessage(), e);
      }
    }
  }

  /**
   * Starts the background flush scheduler for time-triggered windows. No-op when the DAG has no
   * {@link WindowFlushable} node, so ordinary pipelines never spawn a thread. Meant for the
   * long-lived streaming path (see {@code DagExecutor}); the request/response path does not call
   * it.
   */
  public void startFlushScheduler(String callingUser) {
    startFlushScheduler(callingUser, DEFAULT_FLUSH_TICK_MS);
  }

  public synchronized void startFlushScheduler(String callingUser, long tickMs) {
    Preconditions.checkState(
        boundedDefinition == null, "Bounded executions do not run a window timer");
    if (terminated.get() || !hasWindowedNodes || flushScheduler != null) {
      return;
    }
    Preconditions.checkArgument(tickMs > 0, "flush tick must be positive, but was: %s", tickMs);
    // Windowed commands must be initialized before the timer can flush them.
    pipelineLock.lock();
    try {
      initializeAllCommands();
      flushCallingUser = callingUser;
    } finally {
      pipelineLock.unlock();
    }
    flushScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "zephflow-window-flush");
              t.setDaemon(true);
              return t;
            });
    flushTask =
        flushScheduler.scheduleWithFixedDelay(
            this::tickFlush, tickMs, tickMs, TimeUnit.MILLISECONDS);
    log.info(
        "started window flush scheduler with tick {}ms for {} nodes", tickMs, windowedNodes.size());
  }

  private void tickFlush() {
    try {
      flushDueWindows();
    } catch (Exception e) {
      log.error("error during scheduled window flush", e);
    } catch (Error e) {
      log.error("fatal error during scheduled window flush - scheduler will stop", e);
      throw e;
    }
  }

  /**
   * Fires due windows on every {@link WindowFlushable} node and routes their output downstream,
   * reusing the normal traversal so the records reach sinks exactly like {@code process} output.
   * Holds the pipeline lock for the whole pass so it never overlaps {@link #run}.
   */
  private void flushDueWindows() {
    pipelineLock.lock();
    try {
      String callingUser = Objects.requireNonNullElse(flushCallingUser, "");
      Map<String, String> tags = getCallingUserTagAndEventTags(callingUser, null);
      for (Node<OperatorCommand> node : windowedNodes) {
        OperatorCommand command = node.getNodeContent();
        if (!command.isInitialized()) {
          continue; // never processed an event, so it holds no window state to flush
        }
        List<RecordFleakData> output;
        try {
          output =
              ((WindowFlushable) command).flush(callingUser, command.getExecutionContext(), false);
        } catch (Exception e) {
          log.error("window flush failed at node {}", node.getId(), e);
          continue;
        }
        if (CollectionUtils.isEmpty(output)) {
          continue;
        }
        RunContext runContext =
            RunContext.builder()
                .callingUser(callingUser)
                .callingUserTag(tags)
                .dagResult(new DagResult())
                .metricClientProvider(metricClientProvider)
                .runConfig(FLUSH_RUN_CONFIG)
                .build();
        routeToDownstream(
            node.getId(),
            command.commandName(),
            output,
            compiledDagWithoutSource.downstreamEdges(node.getId()),
            runContext);
      }
    } finally {
      pipelineLock.unlock();
    }
  }

  /**
   * Emits the pending output of every initialized {@link EndOfInputFlushable} node in topological
   * order. Each node's output is recorded as its step output, under its first upstream, and routed
   * downstream within {@code runContext}. A failing node is recorded and skipped, so the others
   * still flush.
   */
  private void flushEndOfInputNodes(RunContext runContext) {
    for (Node<OperatorCommand> node : endOfInputNodes) {
      if (runContext.bounded != null) {
        flushBoundedNode(node, runContext);
        continue;
      }
      OperatorCommand command = node.getNodeContent();
      if (!command.isInitialized()) {
        continue;
      }
      String nodeId = node.getId();
      List<RecordFleakData> output;
      try {
        output =
            ((EndOfInputFlushable) command)
                .flushAtEndOfInput(runContext.callingUser, command.getExecutionContext());
      } catch (Exception e) {
        log.error("end-of-input flush failed at node {}", nodeId, e);
        runContext.dagResult.recordFailure(nodeId, command.commandName(), e.getMessage());
        Map<String, String> tags = new HashMap<>(runContext.callingUserTag);
        tags.put(METRIC_TAG_NODE_ID, nodeId);
        tags.put(METRIC_TAG_COMMAND_NAME, command.commandName());
        counters.increaseErrorEventCounter(1, tags);
        continue;
      }
      if (CollectionUtils.isEmpty(output)) {
        continue;
      }
      runContext.dagResult.handleNodeResult(
          runContext.callingUserTag,
          nodeId,
          endOfInputUpstreams.get(nodeId),
          command.commandName(),
          runContext.runConfig,
          output,
          List.of(),
          counters);
      routeToDownstream(
          nodeId,
          command.commandName(),
          output,
          compiledDagWithoutSource.downstreamEdges(nodeId),
          runContext);
    }
  }

  private String firstUpstream(String nodeId) {
    List<Edge> upstream = compiledDagWithoutSource.upstreamEdges(nodeId);
    if (!upstream.isEmpty()) {
      return upstream.getFirst().getFrom();
    }
    return edgesFromSource.stream()
        .filter(e -> e.getTo().equals(nodeId))
        .map(Edge::getFrom)
        .findFirst()
        .orElse(nodeId);
  }

  private static List<Node<OperatorCommand>> topologicalOrder(Dag<OperatorCommand> dag) {
    Map<String, Integer> inDegree = new HashMap<>();
    for (Node<OperatorCommand> node : dag.getNodes()) {
      inDegree.put(node.getId(), dag.upstreamEdges(node.getId()).size());
    }
    Deque<Node<OperatorCommand>> ready = new ArrayDeque<>();
    for (Node<OperatorCommand> node : dag.getNodes()) {
      if (inDegree.get(node.getId()) == 0) {
        ready.add(node);
      }
    }
    List<Node<OperatorCommand>> ordered = new ArrayList<>();
    while (!ready.isEmpty()) {
      Node<OperatorCommand> node = ready.poll();
      ordered.add(node);
      for (Edge edge : dag.downstreamEdges(node.getId())) {
        if (inDegree.merge(edge.getTo(), -1, Integer::sum) == 0) {
          ready.add(dag.lookupNode(edge.getTo()));
        }
      }
    }
    return ordered;
  }

  private synchronized void stopFlushScheduler() {
    if (flushTask != null) {
      flushTask.cancel(false);
      flushTask = null;
    }
    if (flushScheduler != null) {
      flushScheduler.shutdown();
      try {
        if (!flushScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
          log.warn("window flush scheduler did not terminate in time, forcing shutdown");
          flushScheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        log.warn("interrupted while shutting down window flush scheduler");
        flushScheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
      flushScheduler = null;
    }
  }

  private void flushBoundedNode(Node<OperatorCommand> node, RunContext context) {
    OperatorCommand command = node.getNodeContent();
    if (!command.isInitialized()) return;
    BoundedExecution execution = context.bounded;
    execution.checkpoint();
    Invocation invocation = execution.start(node.getId(), null, Phase.END_OF_INPUT, null);
    Invocation previous = execution.enter(invocation);
    List<RecordFleakData> output;
    try {
      output =
          ((EndOfInputFlushable) command)
              .flushAtEndOfInput(context.callingUser, command.getExecutionContext());
      execution.records(invocation, Side.OUTPUT, output);
      execution.finish(invocation, 0, output.size(), true);
    } catch (ExecutionStoppedException failure) {
      throw failure;
    } catch (Exception failure) {
      execution.failed(invocation, failure);
      execution.finish(invocation, 0, 0, false);
      return;
    } finally {
      execution.restore(previous);
    }
    execution.checkpoint();
    if (!output.isEmpty())
      routeToDownstream(
          node.getId(),
          command.commandName(),
          output,
          compiledDagWithoutSource.downstreamEdges(node.getId()),
          context);
  }

  public void terminate() {
    if (boundedDefinition != null) {
      disposeBounded(CompletionDisposition.ABORTED);
      return;
    }
    if (!terminated.compareAndSet(false, true)) {
      return;
    }
    // Stop the timer first (no concurrent flush), then emit all pending output (remaining windows,
    // incomplete sample groups) while sinks are still open, and only then close the commands.
    stopFlushScheduler();
    if (!endOfInputNodes.isEmpty()) {
      try {
        flushAtTermination();
      } catch (Exception e) {
        log.error("final end-of-input flush failed", e);
      }
    }
    if (hasKeyedStatefulNodes) {
      // Closing nulls out execution contexts / keyed state; hold the lock so it can't race a
      // concurrent run() (same contract as the run() lock, for a future multi-threaded source).
      pipelineLock.lock();
      try {
        closeAllCommands();
      } finally {
        pipelineLock.unlock();
      }
    } else {
      closeAllCommands();
    }
  }

  private void flushAtTermination() {
    pipelineLock.lock();
    try {
      String callingUser = Objects.requireNonNullElse(flushCallingUser, "");
      DagResult dagResult = new DagResult();
      flushEndOfInputNodes(
          RunContext.builder()
              .callingUser(callingUser)
              .callingUserTag(getCallingUserTagAndEventTags(callingUser, null))
              .dagResult(dagResult)
              .metricClientProvider(metricClientProvider)
              .runConfig(FLUSH_RUN_CONFIG)
              .build());
      if (dagResult.hasFailure()) {
        DagResult.NodeFailure failure = dagResult.getFirstFailure();
        log.error(
            "final end-of-input flush lost output at node {}: {}",
            failure.nodeId(),
            failure.errorMessage());
      }
    } finally {
      pipelineLock.unlock();
    }
  }

  private void closeAllCommands() {
    compiledDagWithoutSource.getNodes().stream()
        .map(Node::getNodeContent)
        .forEach(
            c -> {
              try {
                c.terminate();
              } catch (Exception e) {
                log.error("failed to terminate command: {}", c, e);
              }
            });
  }

  public record DagRunConfig(boolean includeErrorByStep, boolean includeOutputByStep) {}

  @Builder
  private static class RunContext {
    String callingUser;
    Map<String, String> callingUserTag;
    MetricClientProvider metricClientProvider;
    DagResult dagResult;
    NoSourceDagRunner.DagRunConfig runConfig;
    BoundedExecution bounded;
    String bindingId;
    boolean inParallelBranch;
    CounterDeltas counterDeltas;
  }

  private static final class BranchRun {
    final List<Edge> edges;
    final List<Integer> edgeIndexes;
    final CounterDeltas counterDeltas = new CounterDeltas();
    final RunContext context;
    Throwable failure;
    int firstFailureEdgeIndex = Integer.MAX_VALUE;

    BranchRun(List<Edge> edges, List<Integer> edgeIndexes, RunContext parent) {
      this.edges = edges;
      this.edgeIndexes = edgeIndexes;
      this.context =
          RunContext.builder()
              .callingUser(parent.callingUser)
              .callingUserTag(parent.callingUserTag)
              .metricClientProvider(parent.metricClientProvider)
              .runConfig(parent.runConfig)
              .dagResult(new DagResult())
              .inParallelBranch(true)
              .counterDeltas(counterDeltas)
              .build();
    }

    void recordFailureAt(int edgeIndex) {
      firstFailureEdgeIndex = Math.min(firstFailureEdgeIndex, edgeIndex);
    }
  }

  private static final class CounterDeltas {
    private final List<Consumer<DagRunCounters>> increments = new ArrayList<>();
    final DagRunCounters recorder =
        new DagRunCounters(
            recording(DagRunCounters::increaseInputEventCounter),
            recording(DagRunCounters::increaseInputByteSizeCounter),
            recording(DagRunCounters::increaseTotalDataSizeCounter),
            recording(DagRunCounters::increaseOutputEventCounter),
            recording(DagRunCounters::increaseErrorEventCounter),
            recording(DagRunCounters::increaseNumRequestCounter),
            new MetricClientProvider.NoopMetricClientProvider.NoopStopWatch());

    private FleakCounter recording(CounterIncrement targetIncrement) {
      return new FleakCounter() {
        @Override
        public void increase(Map<String, String> additionalTags) {
          increase(1L, additionalTags);
        }

        @Override
        public void increase(long amount, Map<String, String> additionalTags) {
          increments.add(
              pipelineCounters -> targetIncrement.apply(pipelineCounters, amount, additionalTags));
        }
      };
    }

    void applyTo(DagRunCounters pipelineCounters) {
      increments.forEach(increment -> increment.accept(pipelineCounters));
    }

    private interface CounterIncrement {
      void apply(DagRunCounters pipelineCounters, long amount, Map<String, String> tags);
    }
  }
}
