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

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test-only intermediate command that consumes its whole input population in one call and emits a
 * single {@code {"total": n}} record, or fails the whole batch when configured with {@code fail}.
 */
final class BatchCountCommandFactory extends CommandFactory {
  static final String COMMAND_NAME = "batchcount";

  /** Built-in commands plus {@code batchcount}. */
  static Map<String, CommandFactory> registryWithBatchCount(Map<String, CommandFactory> base) {
    Map<String, CommandFactory> registry = new HashMap<>(base);
    registry.put(COMMAND_NAME, new BatchCountCommandFactory());
    return registry;
  }

  private record Config(String fail) implements CommandConfig {}

  @Override
  public CommandType commandType() {
    return CommandType.INTERMEDIATE_COMMAND;
  }

  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    return new ScalarCommand(
        nodeId,
        jobContext,
        config -> new Config((String) config.get("fail")),
        (config, node, job) -> {}) {
      @Override
      public String commandName() {
        return COMMAND_NAME;
      }

      @Override
      protected ExecutionContext createExecutionContext(
          MetricClientProvider metrics, JobContext job, CommandConfig config, String node) {
        return () -> {};
      }

      @Override
      public ProcessResult process(
          List<RecordFleakData> events, String callingUser, ExecutionContext context) {
        executionCheckpoint();
        String fail = ((Config) commandConfig).fail();
        if (fail != null) {
          var failure = new IllegalStateException(fail);
          return new ProcessResult(
              List.of(),
              events.stream().map(e -> new ErrorOutput(e, failure.getMessage())).toList(),
              executionHooks == null ? null : failure);
        }
        return new ProcessResult(
            List.of((RecordFleakData) FleakData.wrap(Map.of("total", (double) events.size()))),
            List.of());
      }

      @Override
      protected List<RecordFleakData> processOneEvent(
          RecordFleakData event, String callingUser, ExecutionContext context) {
        throw new UnsupportedOperationException("batchcount processes whole populations");
      }
    };
  }
}
