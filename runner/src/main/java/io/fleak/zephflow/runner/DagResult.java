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

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_COMMAND_NAME;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_NODE_ID;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

/** Created by bolei on 3/4/25 */
@Data
public class DagResult {
  Map<String, List<RecordFleakData>> outputEvents = new HashMap<>(); // exit_node -> output_events
  Map<String, Map<String, List<RecordFleakData>>> outputByStep = new HashMap<>();
  Map<String, Map<String, List<ErrorOutput>>> errorByStep = new HashMap<>();

  void handleNodeResult(
      Map<String, String> callingUserTag,
      String nodeId,
      String upstreamNodeId,
      String commandName,
      NoSourceDagRunner.DagRunConfig runConfig,
      List<RecordFleakData> output,
      List<ErrorOutput> failureEvents,
      DagRunCounters pipelineCounters,
      boolean useDlq) {
    if (CollectionUtils.isNotEmpty(failureEvents)) {
      if (useDlq) {
        throw new IllegalArgumentException(failureEvents.getFirst().errorMessage());
      }
      Map<String, String> tags = new HashMap<>(callingUserTag);
      tags.put(METRIC_TAG_NODE_ID, nodeId);
      tags.put(METRIC_TAG_COMMAND_NAME, commandName);
      pipelineCounters.increaseErrorEventCounter(failureEvents.size(), tags);
    }
    if (runConfig.includeErrorByStep()) {
      recordDebugInfo(nodeId, upstreamNodeId, failureEvents, errorByStep);
    }
    if (runConfig.includeOutputByStep()) {
      recordDebugInfo(nodeId, upstreamNodeId, output, outputByStep);
    }
  }

  private <T> void recordDebugInfo(
      String currentNodeId,
      String upstreamNodeId,
      List<T> data,
      Map<String, Map<String, List<T>>> debugDataByStep) {
    Map<String, List<T>> upstreamIdAndDebugInfo =
        debugDataByStep.computeIfAbsent(currentNodeId, k -> new HashMap<>());
    upstreamIdAndDebugInfo.put(upstreamNodeId, data);
  }
}
