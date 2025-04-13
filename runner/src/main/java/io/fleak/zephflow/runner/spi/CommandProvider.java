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
package io.fleak.zephflow.runner.spi;

/* Created by bolei on 4/12/25 */

import io.fleak.zephflow.api.CommandFactory;
import java.util.Map;

/**
 * Service interface for modules wishing to provide command factories to the execution framework.
 * Implementations of this interface can be discovered at runtime using java.util.ServiceLoader.
 */
public interface CommandProvider {
  /**
   * Returns a map of command names to their corresponding factories provided by this specific
   * module or component.
   *
   * @return A non-null, potentially empty, map of command factories. Implementations should handle
   *     potential initialization exceptions internally or document them clearly.
   */
  Map<String, CommandFactory> getCommands();
}
