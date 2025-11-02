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

  /**
   * Process events with explicit execution context.
   *
   * @param events The events to process
   * @param callingUser The calling user ID
   * @param context The execution context (must be initialized via initialize())
   * @return The process result
   */
  public ProcessResult process(
      List<RecordFleakData> events, String callingUser, ExecutionContext context) {
    ProcessResult processResult = new ProcessResult();
    for (RecordFleakData event : events) {
      try {
        List<RecordFleakData> oneOutput = processOneEvent(event, callingUser, context);
        processResult.output.addAll(oneOutput);
      } catch (Exception e) {
        ErrorOutput errorOutput = new ErrorOutput(event, e.getMessage());
        log.debug("process failure: {}", errorOutput, e);
        processResult.failureEvents.add(errorOutput);
      }
    }
    return processResult;
  }

  /**
   * Process a single event. Subclasses should implement this method.
   *
   * @param event The event to process
   * @param callingUser The calling user ID
   * @param context The execution context
   * @return List of output events (may be empty for filtering, or multiple for flattening)
   * @throws Exception if processing fails
   */
  protected abstract List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) throws Exception;

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ProcessResult {
    List<RecordFleakData> output = new ArrayList<>();
    List<ErrorOutput> failureEvents = new ArrayList<>();
  }
}
