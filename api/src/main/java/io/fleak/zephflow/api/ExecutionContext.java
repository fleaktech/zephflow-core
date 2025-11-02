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

import java.io.IOException;

/**
 * Holds runtime resources and state for command execution. This interface replaces the legacy
 * InitializedConfig interface with a clearer name.
 *
 * <p>ExecutionContext instances are created once per command instance during initialization and
 * contain all resources needed for processing events (database connections, counters, executors,
 * etc.).
 *
 * <p>Implementations must be thread-safe if the command will be used in multi-threaded scenarios.
 */
public interface ExecutionContext extends AutoCloseable {

  /** Releases any resources held by this context. Called when the command is terminated. */
  @Override
  void close() throws IOException;
}
