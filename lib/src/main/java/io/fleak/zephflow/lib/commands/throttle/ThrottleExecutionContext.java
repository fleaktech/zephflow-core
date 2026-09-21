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

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.commands.DefaultExecutionContext;
import io.fleak.zephflow.lib.windowing.GroupKeyEvaluator;
import io.fleak.zephflow.lib.windowing.KeyedStateStore;
import lombok.Getter;

@Getter
public class ThrottleExecutionContext extends DefaultExecutionContext {

  static final int CLEANUP_EVERY_N = 10_000;
  static final int IDLE_PERIOD_MULTIPLIER = 2;

  private final FleakCounter droppedCounter;
  private final GroupKeyEvaluator keyEvaluator;
  private final KeyedStateStore<ThrottleState> store;
  private final int numToAllow;
  private final long periodMs;
  private final int cacheSizeLimit;

  private int eventsSinceCleanup;

  public ThrottleExecutionContext(
      FleakCounter inputMessageCounter,
      FleakCounter outputMessageCounter,
      FleakCounter errorCounter,
      FleakCounter droppedCounter,
      GroupKeyEvaluator keyEvaluator,
      KeyedStateStore<ThrottleState> store,
      int numToAllow,
      long periodMs,
      int cacheSizeLimit) {
    super(inputMessageCounter, outputMessageCounter, errorCounter);
    this.droppedCounter = droppedCounter;
    this.keyEvaluator = keyEvaluator;
    this.store = store;
    this.numToAllow = numToAllow;
    this.periodMs = periodMs;
    this.cacheSizeLimit = cacheSizeLimit;
  }

  /** Event-driven cache bounding: every N events, if over the cap, drop idle then over-cap keys. */
  public void maybeEvict(long nowMs) {
    // Reset on cadence (not only when over cap) so the counter can't grow unbounded and overflow.
    if (++eventsSinceCleanup >= CLEANUP_EVERY_N) {
      eventsSinceCleanup = 0;
      if (store.size() > cacheSizeLimit) {
        store.evictIdle(nowMs, IDLE_PERIOD_MULTIPLIER * periodMs);
        store.evictLruBeyond(cacheSizeLimit);
      }
    }
  }

  @Override
  public void close() {}
}
