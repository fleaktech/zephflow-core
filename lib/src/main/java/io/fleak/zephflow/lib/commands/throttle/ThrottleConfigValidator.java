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
package io.fleak.zephflow.lib.commands.throttle;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.windowing.GroupKeyEvaluator;

public class ThrottleConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    ThrottleCommandDto.Config config = (ThrottleCommandDto.Config) commandConfig;
    GroupKeyEvaluator.compile(config.keyExpression()); // throws if the expression is invalid
    Preconditions.checkArgument(
        config.numToAllow() != null && config.numToAllow() >= 1, "numToAllow must be >= 1");
    Preconditions.checkArgument(
        config.periodSeconds() != null && config.periodSeconds() > 0, "periodSeconds must be > 0");
    Preconditions.checkArgument(
        config.cacheSizeLimit() != null && config.cacheSizeLimit() > 0,
        "cacheSizeLimit must be > 0");
  }
}
