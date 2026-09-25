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

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.RandomAware;
import io.fleak.zephflow.lib.commands.sample.SampleExecutionContext.RuleState;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.random.RandomGenerator;

/**
 * Keeps one randomly picked event out of every N events matched by a rule and tags it with N;
 * unmatched events pass through. Per-rule state relies on single-threaded delivery (or the runner's
 * pipeline lock) and is rejected on the request/response path (see {@code DagRunnerService}).
 */
public class SampleCommand extends ScalarCommand implements KeyedStatefulCommand, RandomAware {

  private static final String SAMPLE_DROPPED_COUNT = "sample_dropped_count";

  private RandomGenerator random = new Random();

  public SampleCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_SAMPLE;
  }

  @Override
  public void setRandom(RandomGenerator rng) {
    this.random = rng;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SampleCommandDto.Config config = (SampleCommandDto.Config) commandConfig;
    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    List<SampleCondition> conditions = new ArrayList<>();
    List<RuleState> rules = new ArrayList<>();
    try {
      for (int i = 0; i < config.rules().size(); i++) {
        SampleCommandDto.Rule rule = config.rules().get(i);
        SampleCondition condition = null;
        if (rule.condition() != null) {
          condition = SampleCondition.compile(rule.condition(), i);
          conditions.add(condition);
        }
        rules.add(
            new RuleState(condition == null ? null : condition.expression(), rule.sampleRate()));
      }
    } catch (RuntimeException e) {
      SampleCondition.closeAll(conditions);
      throw e;
    }
    return new SampleExecutionContext(
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags),
        metricClientProvider.counter(METRIC_NAME_OUTPUT_EVENT_COUNT, metricTags),
        metricClientProvider.counter(METRIC_NAME_ERROR_EVENT_COUNT, metricTags),
        metricClientProvider.counter(SAMPLE_DROPPED_COUNT, metricTags),
        rules,
        config.sampleRateField(),
        conditions);
  }

  @Override
  protected List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) {
    SampleExecutionContext ctx = (SampleExecutionContext) context;
    Map<String, String> tags = getCallingUserTagAndEventTags(callingUser, event);
    ctx.getInputMessageCounter().increase(tags);

    RuleState rule = ctx.getRules().stream().filter(r -> r.matches(event)).findFirst().orElse(null);
    if (rule == null) {
      ctx.getOutputMessageCounter().increase(tags);
      return List.of(event);
    }

    int k = ++rule.count;
    if (k == 1) {
      rule.candidate = event;
    } else if (random.nextInt(k) == 0) {
      ctx.getDroppedCounter().increase(getCallingUserTagAndEventTags(callingUser, rule.candidate));
      rule.candidate = event;
    } else {
      ctx.getDroppedCounter().increase(tags);
    }

    if (k < rule.sampleRate) {
      return List.of();
    }
    RecordFleakData kept = rule.candidate;
    rule.candidate = null;
    rule.count = 0;
    ctx.getOutputMessageCounter().increase(getCallingUserTagAndEventTags(callingUser, kept));
    return List.of(
        kept.copyAndMerge(Map.of(ctx.getSampleRateField(), FleakData.wrap(rule.sampleRate))));
  }
}
