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

import static io.fleak.zephflow.lib.utils.ConfigValueUtils.checkNoUnknownKeys;
import static io.fleak.zephflow.lib.utils.ConfigValueUtils.requireInteger;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigParser;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ThrottleConfigParser implements ConfigParser {

  private static final String KEY_EXPRESSION = "keyExpression";
  private static final String NUM_TO_ALLOW = "numToAllow";
  private static final String PERIOD_SECONDS = "periodSeconds";
  private static final String CACHE_SIZE_LIMIT = "cacheSizeLimit";
  private static final Set<String> CONFIG_KEYS =
      new TreeSet<>(Set.of(KEY_EXPRESSION, NUM_TO_ALLOW, PERIOD_SECONDS, CACHE_SIZE_LIMIT));
  private static final long MAX_PERIOD_SECONDS = 365L * 24 * 60 * 60;

  @Override
  public CommandConfig parseConfig(Map<String, Object> config) {
    Preconditions.checkArgument(config != null, "throttle command requires configuration");
    checkNoUnknownKeys(config, CONFIG_KEYS, "");
    Object keyExpression = config.get(KEY_EXPRESSION);
    Preconditions.checkArgument(
        keyExpression instanceof String && !((String) keyExpression).isBlank(),
        "throttle command requires 'keyExpression' to be configured");
    return new ThrottleCommandDto.Config(
        (String) keyExpression,
        (int) integerOrDefault(config, NUM_TO_ALLOW, 1, Integer.MAX_VALUE),
        integerOrDefault(config, PERIOD_SECONDS, 30, MAX_PERIOD_SECONDS),
        (int) integerOrDefault(config, CACHE_SIZE_LIMIT, 50_000, Integer.MAX_VALUE));
  }

  private static long integerOrDefault(
      Map<String, Object> config, String name, long defaultValue, long max) {
    return config.containsKey(name) ? requireInteger(config.get(name), name, 1, max) : defaultValue;
  }
}
