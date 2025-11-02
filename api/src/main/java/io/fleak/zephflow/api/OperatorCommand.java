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

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/** Created by bolei on 7/26/24 */
public abstract class OperatorCommand implements Serializable {

  protected final String nodeId;
  protected final JobContext jobContext;
  private final ConfigParser configParser;
  private final ConfigValidator configValidator;
  private final CommandInitializerFactory commandInitializerFactory;

  protected CommandConfig commandConfig;

  // Explicit initialization approach
  protected transient volatile ExecutionContext executionContext;
  private final transient Object initLock = new Object();

  protected OperatorCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      CommandInitializerFactory commandInitializerFactory) {
    this.nodeId = nodeId;
    this.jobContext = jobContext;
    this.configParser = configParser;
    this.configValidator = configValidator;
    this.commandInitializerFactory = commandInitializerFactory;
  }

  /**
   * This method is called by the compiler at the compilation time. It parses the argument string
   * into a CommandConfig object and validates the config value.
   *
   * @param config The configuration coming from the UI/client
   */
  public void parseAndValidateArg(Map<String, Object> config) {
    commandConfig = configParser.parseConfig(config);
    Preconditions.checkNotNull(commandConfig, "failed to parse input arg: %s", config);
    configValidator.validateConfig(commandConfig, nodeId, jobContext);
  }

  /**
   * @return The command name
   */
  public abstract String commandName();

  /**
   * Explicitly initializes the execution context for this command. This method should be called
   * once before processing any events. Thread-safe via double-checked locking.
   *
   * @param metricClientProvider Provider for metrics
   * @return The initialized execution context
   */
  public ExecutionContext initialize(MetricClientProvider metricClientProvider) {
    if (executionContext == null) {
      synchronized (initLock) {
        if (executionContext == null) {
          String commandName = commandName();
          CommandInitializer commandInitializer =
              commandInitializerFactory.createCommandInitializer(
                  metricClientProvider, jobContext, commandConfig, nodeId);
          executionContext = commandInitializer.initialize(commandName, jobContext, commandConfig);
        }
      }
    }
    return executionContext;
  }

  /**
   * Gets the execution context for this command. Must be initialized via initialize() first.
   *
   * @return The execution context
   * @throws IllegalStateException if context not initialized
   */
  public ExecutionContext getExecutionContext() {
    if (executionContext == null) {
      throw new IllegalStateException(
          "ExecutionContext not initialized for command: "
              + commandName()
              + ". Call initialize() first.");
    }
    return executionContext;
  }

  /** Clean up resources */
  public void terminate() throws IOException {
    if (executionContext != null) {
      executionContext.close();
      executionContext = null;
    }
  }
}
