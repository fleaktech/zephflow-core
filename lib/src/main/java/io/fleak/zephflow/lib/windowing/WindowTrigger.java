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
package io.fleak.zephflow.lib.windowing;

/**
 * Pluggable policy deciding when a key's window should fire. Evaluated both after an event is
 * folded in ({@link KeyedWindowManager#onEvent}) and on each processing-time tick ({@link
 * KeyedWindowManager#onTick}); the same predicate covers both because a count condition can only be
 * met by an {@code add} and a time condition can only be met by the clock advancing.
 */
@FunctionalInterface
public interface WindowTrigger {

  /**
   * @param meta the window's current bookkeeping
   * @param nowMs current processing time (epoch millis)
   * @return {@code true} if the window should fire now
   */
  boolean shouldFire(WindowMeta meta, long nowMs);

  /** Fires once the window has folded in at least {@code maxCount} events. */
  static WindowTrigger count(long maxCount) {
    if (maxCount <= 0) {
      throw new IllegalArgumentException("count trigger maxCount must be positive: " + maxCount);
    }
    return (meta, nowMs) -> meta.count() >= maxCount;
  }

  /**
   * Fires once {@code periodMs} of processing time has elapsed since the window was created. A
   * positive period is required so the window fires on a tick rather than inline on its first
   * event.
   */
  static WindowTrigger time(long periodMs) {
    if (periodMs <= 0) {
      throw new IllegalArgumentException("time trigger periodMs must be positive: " + periodMs);
    }
    return (meta, nowMs) -> (nowMs - meta.createdAtMs()) >= periodMs;
  }

  /** Fires as soon as any of the given triggers fires. */
  static WindowTrigger any(WindowTrigger... triggers) {
    return (meta, nowMs) -> {
      for (WindowTrigger t : triggers) {
        if (t.shouldFire(meta, nowMs)) {
          return true;
        }
      }
      return false;
    };
  }
}
