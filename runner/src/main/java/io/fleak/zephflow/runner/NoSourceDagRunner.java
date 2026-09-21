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
import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.api.KeyedStatefulCommand;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.WindowFlushable;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.NodeExecutionException;
import io.fleak.zephflow.runner.dag.Dag;
import io.fleak.zephflow.runner.dag.Edge;
import io.fleak.zephflow.runner.dag.Node;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.MDC;

/**
 * Created by bolei on 3/4/25
 *
 * <p>Single-threaded, synchronous DFS over the source-less DAG: each {@link #run} walks the graph
 * on the caller's thread.
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
 */
@Slf4j
public class NoSourceDagRunner {

  private static final long DEFAULT_FLUSH_TICK_MS = 1000L;
  private static final DagRunConfig FLUSH_RUN_CONFIG = new DagRunConfig(false, false);

  @NonNull private final List<Edge> edgesFromSource;
  private final Dag<OperatorCommand> compiledDagWithoutSource;
  private final MetricClientProvider metricClientProvider;
  private final DagRunCounters counters;
  private final boolean useDlq;

  private final List<Node<OperatorCommand>> windowedNodes;
  private final boolean hasWindowedNodes;
  private final boolean hasKeyedStatefulNodes;
  private final ReentrantLock pipelineLock = new ReentrantLock();
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
    this.edgesFromSource = edgesFromSource;
    this.compiledDagWithoutSource = compiledDagWithoutSource;
    this.metricClientProvider = metricClientProvider;
    this.counters = counters;
    this.useDlq = useDlq;
    this.windowedNodes =
        compiledDagWithoutSource.getNodes().stream()
            .filter(n -> n.getNodeContent() instanceof WindowFlushable)
            .toList();
    this.hasWindowedNodes = !windowedNodes.isEmpty();
    this.hasKeyedStatefulNodes =
        compiledDagWithoutSource.getNodes().stream()
            .anyMatch(n -> n.getNodeContent() instanceof KeyedStatefulCommand);
  }

  public DagResult run(
      List<RecordFleakData> events, String callingUser, NoSourceDagRunner.DagRunConfig runConfig) {
    // Any keyed-stateful node (windowed aggregation or throttle) holds non-thread-safe per-key
    // state, so take the lock to serialize run() against the flush thread and any concurrent
    // run() caller. Stateless pipelines skip the lock entirely (zero overhead, single-threaded).
    if (!hasKeyedStatefulNodes) {
      return doRun(events, callingUser, runConfig);
    }
    pipelineLock.lock();
    try {
      return doRun(events, callingUser, runConfig);
    } finally {
      pipelineLock.unlock();
    }
  }

  private DagResult doRun(
      List<RecordFleakData> events, String callingUser, NoSourceDagRunner.DagRunConfig runConfig) {

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
    routeToDownstream(sourceNodeId, commandName, events, edgesFromSource, runContext);
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
      List<RecordFleakData> currentNodeOutput =
          runContext.dagResult.outputEvents.computeIfAbsent(currentNodeId, k -> new ArrayList<>());
      currentNodeOutput.addAll(events);
      Map<String, String> tags = new HashMap<>(runContext.callingUserTag);
      tags.put(METRIC_TAG_NODE_ID, currentNodeId);
      tags.put(METRIC_TAG_COMMAND_NAME, commandName);
      counters.increaseOutputEventCounter(events.size(), tags);
      return;
    }
    for (var e : outgoingEdges) {
      // Failure isolation: a node failure aborts only its own subtree; sibling branches still run.
      // Recorded failures may trigger the DLQ once the whole run finishes (see run()).
      // Data isolation relies on the command contract (no in-place mutation of input records), so
      // fan-out branches safely share the same event objects with no per-branch copy.
      try {
        processEvent(e.getTo(), currentNodeId, events, runContext);
      } catch (NodeExecutionException nee) {
        runContext.dagResult.recordFailure(nee.getNodeId(), nee.getCommandName(), nee.getMessage());
        Map<String, String> tags = new HashMap<>(runContext.callingUserTag);
        tags.put(METRIC_TAG_NODE_ID, nee.getNodeId());
        tags.put(METRIC_TAG_COMMAND_NAME, nee.getCommandName());
        counters.increaseErrorEventCounter(events.size(), tags);
        log.debug("node {} failed; isolating branch", nee.getNodeId(), nee);
      }
    }
  }

  void processEvent(
      String currentNodeId,
      String upstreamNodeId,
      List<RecordFleakData> events,
      RunContext runContext) {
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
            counters);

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
            counters);
        Map<String, String> tags = new HashMap<>(runContext.callingUserTag);
        tags.put(METRIC_TAG_NODE_ID, currentNodeId);
        tags.put(METRIC_TAG_COMMAND_NAME, command.commandName());
        counters.increaseOutputEventCounter(result.getSuccessCount(), tags);

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
      flushWindowedNodes(false);
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
  private void flushWindowedNodes(boolean finalFlush) {
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
              ((WindowFlushable) command)
                  .flush(callingUser, command.getExecutionContext(), finalFlush);
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

  public void terminate() {
    if (!terminated.compareAndSet(false, true)) {
      return;
    }
    // Stop the timer first (no concurrent flush), then drain every remaining window while sinks are
    // still open, and only then close the commands.
    stopFlushScheduler();
    if (hasWindowedNodes) {
      try {
        flushWindowedNodes(true);
      } catch (Exception e) {
        log.error("final window flush failed", e);
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
  }
}
