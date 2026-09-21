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
import io.fleak.zephflow.api.ConfigParser;
import java.util.Map;

public class ThrottleConfigParser implements ConfigParser {

  @Override
  public CommandConfig parseConfig(Map<String, Object> config) {
    Preconditions.checkArgument(config != null, "throttle command requires configuration");
    Object keyExpression = config.get("keyExpression");
    Preconditions.checkArgument(
        keyExpression instanceof String && !((String) keyExpression).isBlank(),
        "throttle command requires 'keyExpression' to be configured");
    return new ThrottleCommandDto.Config(
        (String) keyExpression,
        toInt(config.get("numToAllow"), 1),
        toLong(config.get("periodSeconds"), 30L),
        toInt(config.get("cacheSizeLimit"), 50_000));
  }

  private static int toInt(Object value, int defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    return value instanceof Number n ? n.intValue() : Integer.parseInt(value.toString().trim());
  }

  private static long toLong(Object value, long defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    return value instanceof Number n ? n.longValue() : Long.parseLong(value.toString().trim());
  }
}
