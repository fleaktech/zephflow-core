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
package io.fleak.zephflow.api;

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class ScalarCommand extends OperatorCommand {

  protected ScalarCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      CommandInitializerFactory commandInitializerFactory) {
    super(nodeId, jobContext, configParser, configValidator, commandInitializerFactory);
  }

  public ScalarCommand.ProcessResult process(
      List<RecordFleakData> events, String callingUser, MetricClientProvider metricClientProvider) {
    lazyInitialize(metricClientProvider);
    InitializedConfig initializedConfig = initializedConfigThreadLocal.get();
    ProcessResult processResult = new ProcessResult();
    for (RecordFleakData event : events) {
      try {
        List<RecordFleakData> oneOutput = processOneEvent(event, callingUser, initializedConfig);
        processResult.output.addAll(oneOutput);
      } catch (Exception e) {
        log.debug("process failure", e);
        ErrorOutput errorOutput = new ErrorOutput(event, e.getMessage());
        processResult.failureEvents.add(errorOutput);
      }
    }
    return processResult;
  }

  protected abstract List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, InitializedConfig initializedConfig)
      throws Exception;

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ProcessResult {
    List<RecordFleakData> output = new ArrayList<>();
    List<ErrorOutput> failureEvents = new ArrayList<>();
  }
}
