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
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.runner.dag.Dag;
import io.fleak.zephflow.runner.dag.Edge;
import io.fleak.zephflow.runner.dag.Node;
import java.util.*;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.MDC;

/** Created by bolei on 3/4/25 */
@Slf4j
public record NoSourceDagRunner(
    @NonNull List<Edge> edgesFromSource,
    Dag<OperatorCommand> compiledDagWithoutSource,
    MetricClientProvider metricClientProvider,
    DagRunCounters counters,
    boolean useDlq) {

  public DagResult run(
      List<RecordFleakData> events, String callingUser, NoSourceDagRunner.DagRunConfig runConfig) {

    // make sure all edges are from the same source
    var sourceNodeIds = edgesFromSource.stream().map(Edge::getFrom).distinct().toList();
    Preconditions.checkArgument(
        sourceNodeIds.size() == 1,
        String.format(
            "Only single source DAG is supported but found %d sources", sourceNodeIds.size()));
    var sourceNodeId = sourceNodeIds.get(0);
    String commandName = "source_node";

    Map<String, String> callingUserTag = getCallingUserTag(callingUser);

    Map<String, String> enrichedTags = getTagsFromEvent(events, callingUserTag);

    counters.increaseInputEventCounter(events.size(), callingUserTag);
    counters.startStopWatch();
    MDC.put("callingUser", callingUser);
    log.debug("events {}", events.size());

    DagResult dagResult = new DagResult();
    RunContext runContext =
        RunContext.builder()
            .callingUser(callingUser)
            .callingUserTag(callingUserTag)
            .dagResult(dagResult)
            .metricClientProvider(metricClientProvider)
            .runConfig(runConfig)
            .build();
    routeToDownstream(sourceNodeId, commandName, events, edgesFromSource, runContext);
    counters.stopStopWatch(enrichedTags);
    MDC.clear();
    dagResult.consolidateSinkResult(); // merge all sinkResults and put them into outputEvents
    if (MapUtils.isNotEmpty(dagResult.getErrorByStep())) {
      log.error("failed to process events: {}", toJsonString(dagResult.errorByStep));
    }
    return dagResult;
  }

  private Map<String, String> getTagsFromEvent(
      List<RecordFleakData> events, Map<String, String> callingUserTag) {
    Map<String, String> tags = new HashMap<>(callingUserTag);

    events.stream()
        .findFirst()
        .map(event -> event.getPayload().get("__tag__"))
        .map(FleakData::unwrap)
        .filter(Map.class::isInstance)
        .map(obj -> (Map<String, Object>) obj)
        .ifPresent(
            tagMap ->
                tagMap.entrySet().stream()
                    .filter(entry -> entry.getValue() != null)
                    .forEach(entry -> tags.put(entry.getKey(), entry.getValue().toString())));

    return tags;
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
      processEvent(e.getTo(), currentNodeId, events, runContext);
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
    if (command instanceof ScalarCommand scalarCommand) {
      // Process the event through a scalar command
      ScalarCommand.ProcessResult result =
          scalarCommand.process(events, runContext.callingUser, runContext.metricClientProvider);
      runContext.dagResult.handleNodeResult(
          runContext.callingUserTag,
          currentNodeId,
          upstreamNodeId,
          command.commandName(),
          runContext.runConfig,
          result.getOutput(),
          result.getFailureEvents(),
          counters,
          useDlq);

      routeToDownstream(
          currentNodeId, command.commandName(), result.getOutput(), downstreamEdges, runContext);
      return;
    }
    if (command instanceof ScalarSinkCommand sinkCommand) {
      // Write to sink
      ScalarSinkCommand.SinkResult result =
          sinkCommand.writeToSink(events, runContext.callingUser, runContext.metricClientProvider);
      RecordFleakData sinkOutputEvent = sinkResultToOutputEvent(result);
      runContext.dagResult.handleNodeResult(
          runContext.callingUserTag,
          currentNodeId,
          upstreamNodeId,
          command.commandName(),
          runContext.runConfig,
          List.of(sinkOutputEvent),
          result.getFailureEvents(),
          counters,
          useDlq);
      Map<String, String> tags = new HashMap<>(runContext.callingUserTag);
      tags.put(METRIC_TAG_NODE_ID, currentNodeId);
      tags.put(METRIC_TAG_COMMAND_NAME, command.commandName());
      counters.increaseOutputEventCounter(events.size(), tags);

      if (runContext.dagResult.sinkResultMap.containsKey(currentNodeId)) {
        result.merge(runContext.dagResult.sinkResultMap.get(currentNodeId));
      }
      runContext.dagResult.sinkResultMap.put(currentNodeId, result);
      return;
    }
    throw new IllegalStateException(
        String.format(
            "encountered unsupported command at downstream node: id=%s, commandName=%s",
            currentNodeId, command.commandName()));
  }

  public void terminate() {
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
