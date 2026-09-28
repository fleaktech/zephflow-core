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

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_ENV;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ThrottleCommandTest {

  private static final JobContext JOB_CONTEXT =
      JobContext.builder()
          .metricTags(Map.of(METRIC_TAG_SERVICE, "test", METRIC_TAG_ENV, "test"))
          .build();

  private static ThrottleCommand command(Map<String, Object> config) {
    ThrottleCommand cmd =
        (ThrottleCommand) new ThrottleCommandFactory().createCommand("n1", JOB_CONTEXT);
    cmd.parseAndValidateArg(config);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    return cmd;
  }

  private static RecordFleakData rec(Map<String, Object> m) {
    return (RecordFleakData) FleakData.wrap(m);
  }

  private static List<RecordFleakData> run(ThrottleCommand cmd, List<RecordFleakData> events) {
    ScalarCommand.ProcessResult r = cmd.process(events, "u", cmd.getExecutionContext());
    assertEquals(List.of(), r.getFailureEvents());
    return r.getOutput();
  }

  @Test
  void m1_allowsFirstPerKeyInBatch_dropsRest_keysIndependent() {
    ThrottleCommand cmd =
        command(Map.of("keyExpression", "$.host", "numToAllow", 1, "periodSeconds", 3600));
    RecordFleakData a = rec(Map.of("host", "a"));
    RecordFleakData b = rec(Map.of("host", "b"));
    // a,a,a,b,b in one batch: first a and first b pass, the rest are throttled
    assertEquals(List.of(a, b), run(cmd, List.of(a, a, a, b, b)));
  }

  @Test
  void m2_allowsFirstTwoPerKeyInBatch() {
    ThrottleCommand cmd =
        command(Map.of("keyExpression", "$.host", "numToAllow", 2, "periodSeconds", 3600));
    RecordFleakData a = rec(Map.of("host", "a"));
    assertEquals(List.of(a, a), run(cmd, List.of(a, a, a, a)));
  }

  @Test
  void stampsThrottledCountOnFirstEventAfterDropPhase() {
    ThrottleCommand cmd =
        command(Map.of("keyExpression", "$.host", "numToAllow", 1, "periodSeconds", 30));
    long[] now = {0L};
    cmd.setClock(() -> now[0]);
    RecordFleakData a = rec(Map.of("host", "a"));

    now[0] = 0L;
    assertEquals(List.of(a), run(cmd, List.of(a))); // passes, starts a 30s drop-phase
    now[0] = 1_000L;
    assertEquals(List.of(), run(cmd, List.of(a))); // dropped
    now[0] = 2_000L;
    assertEquals(List.of(), run(cmd, List.of(a))); // dropped (2 total)
    now[0] = 31_000L; // past the period: passes and carries the previous phase's drop count
    RecordFleakData stamped = rec(Map.of("host", "a", "throttledCount", 2L));
    assertEquals(List.of(stamped), run(cmd, List.of(a)));
  }

  @Test
  void maxPeriod_oneYear_throttlesAcrossTheWholeYear_atRealEpochTime() {
    ThrottleCommand cmd =
        command(Map.of("keyExpression", "$.host", "numToAllow", 1, "periodSeconds", 31_536_000));
    long start = 1_790_000_000_000L;
    long oneYearMs = 31_536_000_000L;
    long[] now = {start};
    cmd.setClock(() -> now[0]);
    RecordFleakData a = rec(Map.of("host", "a"));

    assertEquals(List.of(a), run(cmd, List.of(a)));
    now[0] = start + oneYearMs - 1;
    assertEquals(List.of(), run(cmd, List.of(a)));
    now[0] = start + oneYearMs;
    assertEquals(List.of(rec(Map.of("host", "a", "throttledCount", 1L))), run(cmd, List.of(a)));
  }

  @Test
  void maxNumToAllowAndCacheSize_allEventsPass() {
    ThrottleCommand cmd =
        command(
            Map.of(
                "keyExpression",
                "$.host",
                "numToAllow",
                Integer.MAX_VALUE,
                "periodSeconds",
                31_536_000,
                "cacheSizeLimit",
                Integer.MAX_VALUE));
    cmd.setClock(() -> 1_790_000_000_000L);
    RecordFleakData a = rec(Map.of("host", "a"));
    List<RecordFleakData> events =
        Collections.nCopies(ThrottleExecutionContext.CLEANUP_EVERY_N + 1, a);
    assertEquals(events, run(cmd, events));
  }

  @Test
  void maxPeriod_idleEvictionRemovesKeysIdleForTwoPeriods() {
    ThrottleCommand cmd =
        command(
            Map.of(
                "keyExpression", "$.host",
                "numToAllow", 1,
                "periodSeconds", 31_536_000,
                "cacheSizeLimit", 2));
    long start = 1_790_000_000_000L;
    long[] now = {start};
    cmd.setClock(() -> now[0]);
    RecordFleakData a = rec(Map.of("host", "a"));
    RecordFleakData b = rec(Map.of("host", "b"));
    RecordFleakData c = rec(Map.of("host", "c"));

    assertEquals(List.of(b, c), run(cmd, List.of(b, c, c)));
    now[0] = start + 2 * 31_536_000_000L;
    int aEvents = ThrottleExecutionContext.CLEANUP_EVERY_N - 3;
    assertEquals(List.of(a), run(cmd, Collections.nCopies(aEvents, a)));
    assertEquals(List.of(c), run(cmd, List.of(c)));
  }

  @Test
  void failOpen_missingKey_passesThrough() {
    ThrottleCommand cmd =
        command(Map.of("keyExpression", "$.host", "numToAllow", 1, "periodSeconds", 3600));
    RecordFleakData noHost = rec(Map.of("other", 1));
    assertEquals(List.of(noHost, noHost, noHost), run(cmd, List.of(noHost, noHost, noHost)));
  }

  @Test
  void failOpen_nonScalarKey_passesThrough() {
    ThrottleCommand cmd =
        command(Map.of("keyExpression", "$.obj", "numToAllow", 1, "periodSeconds", 3600));
    RecordFleakData obj = rec(Map.of("obj", Map.of("x", 1)));
    assertEquals(List.of(obj, obj), run(cmd, List.of(obj, obj)));
  }

  @Test
  void failOpen_keyEvalError_passesThrough() {
    ThrottleCommand cmd =
        command(
            Map.of("keyExpression", "parse_int($.host)", "numToAllow", 1, "periodSeconds", 3600));
    RecordFleakData bad = rec(Map.of("host", "not-a-number"));
    assertEquals(List.of(bad, bad), run(cmd, List.of(bad, bad)));
  }
}
