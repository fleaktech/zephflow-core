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
import java.math.BigInteger;
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
    for (Object key : config.keySet()) {
      Preconditions.checkArgument(
          key instanceof String name && CONFIG_KEYS.contains(name),
          "unknown parameter '%s'; allowed: %s",
          key,
          CONFIG_KEYS);
    }
    Object keyExpression = config.get(KEY_EXPRESSION);
    Preconditions.checkArgument(
        keyExpression instanceof String && !((String) keyExpression).isBlank(),
        "throttle command requires 'keyExpression' to be configured");
    return new ThrottleCommandDto.Config(
        (String) keyExpression,
        (int) parseInteger(config, NUM_TO_ALLOW, 1, Integer.MAX_VALUE),
        parseInteger(config, PERIOD_SECONDS, 30, MAX_PERIOD_SECONDS),
        (int) parseInteger(config, CACHE_SIZE_LIMIT, 50_000, Integer.MAX_VALUE));
  }

  private static long parseInteger(
      Map<String, Object> config, String name, long defaultValue, long max) {
    if (!config.containsKey(name)) {
      return defaultValue;
    }
    Object value = config.get(name);
    BigInteger n =
        switch (value) {
          case Integer i -> BigInteger.valueOf(i);
          case Long l -> BigInteger.valueOf(l);
          case Short s -> BigInteger.valueOf(s);
          case Byte b -> BigInteger.valueOf(b);
          case BigInteger b -> b;
          case null, default ->
              throw new IllegalArgumentException(
                  String.format(
                      "'%s' must be an integer, got: %s",
                      name, value instanceof String str ? "\"" + str + "\"" : value));
        };
    Preconditions.checkArgument(
        n.signum() > 0 && n.compareTo(BigInteger.valueOf(max)) <= 0,
        "'%s' must be between 1 and %s, got: %s",
        name,
        max,
        n);
    return n.longValueExact();
  }
}
