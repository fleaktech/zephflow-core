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
 * Per-key throttle state and its pure decision logic (duty cycle): allow up to {@code numToAllow}
 * events, then drop for {@code periodMs} after the M-th allowed event; the first event after a
 * drop-phase carries the previous phase's drop count. Time is an explicit parameter (no clock read
 * here) so the logic is deterministically unit-testable.
 */
public final class ThrottleState {

  private int allowed;
  private int dropped;
  private long suppressUntilMs; // 0 = not currently throttling

  /**
   * @param pass whether this event is allowed through
   * @param stamp whether to stamp the drop count (only on the first event after a drop-phase)
   * @param droppedCount number of events dropped in the just-ended drop-phase (valid when stamp)
   */
  public record Decision(boolean pass, boolean stamp, long droppedCount) {}

  public Decision decide(long nowMs, int numToAllow, long periodMs) {
    if (suppressUntilMs != 0 && nowMs < suppressUntilMs) {
      dropped++;
      return new Decision(false, false, 0);
    }
    boolean stamp = false;
    long stampCount = 0;
    if (suppressUntilMs != 0) { // drop-phase just expired
      if (dropped > 0) {
        stamp = true;
        stampCount = dropped;
      }
      suppressUntilMs = 0;
      allowed = 0;
      dropped = 0;
    }
    allowed++;
    if (allowed >= numToAllow) {
      suppressUntilMs = nowMs + periodMs;
    }
    return new Decision(true, stamp, stampCount);
  }
}
