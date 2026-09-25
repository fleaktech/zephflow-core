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
package io.fleak.zephflow.lib.commands.sample;

import static io.fleak.zephflow.lib.commands.sample.SampleCommandDto.DEFAULT_SAMPLE_RATE_FIELD;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigParser;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class SampleConfigParser implements ConfigParser {

  private static final String RULES = "rules";
  private static final String CONDITION = "condition";
  private static final String SAMPLE_RATE = "sampleRate";
  private static final String SAMPLE_RATE_FIELD = "sampleRateField";
  private static final Set<String> CONFIG_KEYS = new TreeSet<>(Set.of(RULES, SAMPLE_RATE_FIELD));
  private static final Set<String> RULE_KEYS = new TreeSet<>(Set.of(CONDITION, SAMPLE_RATE));

  @Override
  public CommandConfig parseConfig(Map<String, Object> config) {
    Preconditions.checkArgument(config != null, "sample command requires 'rules' to be configured");
    checkNoUnknownKeys(config, CONFIG_KEYS, "");
    Object rawRules = config.get(RULES);
    Preconditions.checkArgument(
        rawRules instanceof List<?> list && !list.isEmpty(),
        "sample command requires 'rules' to be a non-empty list");
    List<SampleCommandDto.Rule> rules = new ArrayList<>();
    List<?> ruleList = (List<?>) rawRules;
    for (int i = 0; i < ruleList.size(); i++) {
      rules.add(parseRule(ruleList.get(i), i));
    }
    return new SampleCommandDto.Config(rules, parseSampleRateField(config));
  }

  private static SampleCommandDto.Rule parseRule(Object rawRule, int index) {
    Preconditions.checkArgument(
        rawRule instanceof Map<?, ?>, "'rules[%s]' must be an object", index);
    Map<?, ?> rule = (Map<?, ?>) rawRule;
    checkNoUnknownKeys(rule, RULE_KEYS, String.format("rules[%s].", index));
    Object condition = rule.get(CONDITION);
    Preconditions.checkArgument(
        condition == null || (condition instanceof String s && !s.isBlank()),
        "'rules[%s].condition' must be a non-blank string",
        index);
    return new SampleCommandDto.Rule(
        (String) condition, parseSampleRate(rule.get(SAMPLE_RATE), index));
  }

  private static void checkNoUnknownKeys(Map<?, ?> map, Set<String> allowed, String prefix) {
    for (Object key : map.keySet()) {
      Preconditions.checkArgument(
          key instanceof String name && allowed.contains(name),
          "unknown parameter '%s%s'; allowed: %s",
          prefix,
          key,
          allowed);
    }
  }

  private static int parseSampleRate(Object value, int index) {
    BigInteger rate =
        switch (value) {
          case Integer n -> BigInteger.valueOf(n);
          case Long n -> BigInteger.valueOf(n);
          case Short n -> BigInteger.valueOf(n);
          case Byte n -> BigInteger.valueOf(n);
          case BigInteger n -> n;
          case null, default ->
              throw new IllegalArgumentException(
                  String.format(
                      "'rules[%s].sampleRate' must be an integer, got: %s", index, value));
        };
    Preconditions.checkArgument(
        rate.signum() > 0 && rate.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) <= 0,
        "'rules[%s].sampleRate' must be between 1 and %s, got: %s",
        index,
        Integer.MAX_VALUE,
        rate);
    return rate.intValueExact();
  }

  private static String parseSampleRateField(Map<String, Object> config) {
    if (!config.containsKey(SAMPLE_RATE_FIELD)) {
      return DEFAULT_SAMPLE_RATE_FIELD;
    }
    Object field = config.get(SAMPLE_RATE_FIELD);
    Preconditions.checkArgument(
        field instanceof String s && !s.isBlank(), "'sampleRateField' must be a non-blank string");
    return (String) field;
  }
}
