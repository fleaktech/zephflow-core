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
package io.fleak.zephflow.lib.commands.noop;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_NOOP;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.Collections;
import java.util.List;

public class NoopCommand extends ScalarCommand {

  public NoopCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_NOOP;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      io.fleak.zephflow.api.metric.MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    return new NoopExecutionContext();
  }

  @Override
  public List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) {
    System.out.printf("noop nodeId: %s, event: %s%n", nodeId, toJsonString(event));
    return Collections.singletonList(event);
  }

  private static class NoopExecutionContext implements ExecutionContext {
    @Override
    public void close() {}
  }
}
