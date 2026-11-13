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
package io.fleak.zephflow.lib.commands.eval;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigParser;
import io.fleak.zephflow.api.ConfigurationValidationException;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/** Created by bolei on 10/20/24 */
@AllArgsConstructor
@NoArgsConstructor
public class EvalConfigParser implements ConfigParser {
  private boolean assertion;

  @Override
  public CommandConfig parseConfig(Map<String, Object> config) {
    String eval;
    try {
      eval = (String) config.get("expression");
    } catch (ClassCastException failure) {
      throw ConfigurationValidationException.at(
          of("expression"), failure, "Expression must be a string");
    }
    require(
        eval != null,
        of("expression"),
        "Expression is required",
        "eval command requires 'expression' to be configured");
    return new EvalCommandDto.Config(eval, assertion);
  }
}
