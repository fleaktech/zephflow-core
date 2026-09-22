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

/**
 * Per-key throttle state and its pure decision logic (fixed period): allow up to {@code numToAllow}
 * events within a {@code periodMs} window <b>anchored at the first event of the period</b>, drop
 * the rest, then open a fresh window on the first event at or after the window end. That first
 * event of the next period carries the previous period's drop count. Time is an explicit parameter
 * (no clock read here) so the logic is deterministically unit-testable.
 *
 * <p>The window is anchored at the period's first event, not at the M-th allowed one, mirroring
 * Cribl's Suppress function (allow M per key per period). For {@code numToAllow == 1} this is
 * indistinguishable from a duty cycle; the two diverge only when the M-th allowed event arrives
 * late within the window (see FLE-2792).
 */
public final class ThrottleState {

  private int allowed;
  private int dropped;
  private long windowEndMs; // 0 = no active window yet

  /**
   * @param pass whether this event is allowed through
   * @param stamp whether to stamp the drop count (only on the first event of a period that follows
   *     a period in which events were dropped)
   * @param droppedCount number of events dropped in the just-ended period (valid when stamp)
   */
  public record Decision(boolean pass, boolean stamp, long droppedCount) {}

  public Decision decide(long nowMs, int numToAllow, long periodMs) {
    boolean stamp = false;
    long stampCount = 0;
    if (windowEndMs == 0 || nowMs >= windowEndMs) { // open a new period (half-open [start, end))
      if (windowEndMs != 0 && dropped > 0) { // the previous period ended with drops
        stamp = true;
        stampCount = dropped;
      }
      windowEndMs = nowMs + periodMs;
      allowed = 0;
      dropped = 0;
    }
    if (allowed < numToAllow) {
      allowed++;
      return new Decision(true, stamp, stampCount);
    }
    dropped++;
    return new Decision(false, false, 0);
  }
}
