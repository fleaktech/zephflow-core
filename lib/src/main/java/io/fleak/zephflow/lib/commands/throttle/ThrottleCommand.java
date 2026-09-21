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

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.windowing.GroupKeyEvaluator;
import io.fleak.zephflow.lib.windowing.GroupKeyEvaluator.GroupKey;
import io.fleak.zephflow.lib.windowing.KeyedStateStore;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Per-key rate limiter (duty cycle): allows M events per key, then drops for T seconds after the
 * M-th allowed event, and stamps the first passing event after a drop-phase with {@code
 * throttledCount}. Event-driven (no flush scheduler). Fail-open: events whose key expression yields
 * no usable scalar pass through unthrottled.
 *
 * <p>Per-key state is not thread-safe and relies on single-threaded event delivery from the source
 * (or serialization by the runner's pipeline lock when the DAG also has a windowed node). It is
 * rejected on the request/response path (see {@code DagRunnerService}).
 */
public class ThrottleCommand extends ScalarCommand implements KeyedStatefulCommand {

  private static final String THROTTLE_DROPPED_COUNT = "throttle_dropped_count";
  static final String THROTTLED_COUNT_FIELD = "throttledCount";

  // Processing-time source; overridable in tests to drive the period boundary deterministically.
  private LongSupplier clock = System::currentTimeMillis;

  public ThrottleCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_THROTTLE;
  }

  void setClock(LongSupplier clock) {
    this.clock = clock;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    ThrottleCommandDto.Config config = (ThrottleCommandDto.Config) commandConfig;
    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    FleakCounter inputCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter outputCounter =
        metricClientProvider.counter(METRIC_NAME_OUTPUT_EVENT_COUNT, metricTags);
    FleakCounter errorCounter =
        metricClientProvider.counter(METRIC_NAME_ERROR_EVENT_COUNT, metricTags);
    FleakCounter droppedCounter = metricClientProvider.counter(THROTTLE_DROPPED_COUNT, metricTags);
    return new ThrottleExecutionContext(
        inputCounter,
        outputCounter,
        errorCounter,
        droppedCounter,
        GroupKeyEvaluator.compile(config.keyExpression()),
        new KeyedStateStore<>(),
        config.numToAllow(),
        config.periodSeconds() * 1000L,
        config.cacheSizeLimit());
  }

  @Override
  protected List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) {
    ThrottleExecutionContext ctx = (ThrottleExecutionContext) context;
    Map<String, String> tags = getCallingUserTagAndEventTags(callingUser, event);
    ctx.getInputMessageCounter().increase(tags);

    GroupKey key = ctx.getKeyEvaluator().evaluate(event);
    if (key.kind() != GroupKeyEvaluator.Kind.PRESENT) {
      // fail-open: cannot cleanly key this event, so pass it through unthrottled
      if (key.kind() == GroupKeyEvaluator.Kind.ERROR) {
        ctx.getErrorCounter().increase(tags);
      }
      ctx.getOutputMessageCounter().increase(tags);
      return List.of(event);
    }

    long nowMs = clock.getAsLong();
    ThrottleState state =
        ctx.getStore().getOrCreate(key.value(), nowMs, ThrottleState::new).state();
    ctx.maybeEvict(nowMs);
    ThrottleState.Decision decision = state.decide(nowMs, ctx.getNumToAllow(), ctx.getPeriodMs());

    if (!decision.pass()) {
      ctx.getDroppedCounter().increase(tags);
      return List.of();
    }
    ctx.getOutputMessageCounter().increase(tags);
    if (decision.stamp()) {
      return List.of(
          event.copyAndMerge(
              Map.of(THROTTLED_COUNT_FIELD, FleakData.wrap(decision.droppedCount()))));
    }
    return List.of(event);
  }
}
