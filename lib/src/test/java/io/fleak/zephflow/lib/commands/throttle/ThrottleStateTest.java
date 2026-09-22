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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.lib.commands.throttle.ThrottleState.Decision;
import org.junit.jupiter.api.Test;

class ThrottleStateTest {

  private static final long T = 30_000L; // 30s in millis

  private static Decision pass() {
    return new Decision(true, false, 0);
  }

  private static Decision passStamped(long dropped) {
    return new Decision(true, true, dropped);
  }

  private static Decision drop() {
    return new Decision(false, false, 0);
  }

  @Test
  void m1_allowsOnePerPeriod_thenStampsDropCountOnFirstEventAfterPeriod() {
    ThrottleState s = new ThrottleState();
    assertEquals(pass(), s.decide(0, 1, T)); // first passes, starts drop-phase until T
    assertEquals(drop(), s.decide(5, 1, T));
    assertEquals(drop(), s.decide(10, 1, T)); // 2 dropped
    // now == suppressUntil (half-open [start, until)) -> passes, carries dropped=2, re-arms
    assertEquals(passStamped(2), s.decide(T, 1, T));
    assertEquals(drop(), s.decide(T + 1, 1, T));
  }

  @Test
  void m2_allowsTwoThenSuppresses_stampsOnlyFirstPostDropEvent() {
    ThrottleState s = new ThrottleState();
    assertEquals(pass(), s.decide(0, 2, T)); // window [0, T), allowed 1
    assertEquals(pass(), s.decide(1, 2, T)); // still in [0, T), allowed 2
    assertEquals(drop(), s.decide(2, 2, T)); // over M within the window -> dropped 1
    assertEquals(passStamped(1), s.decide(1 + T, 2, T)); // past T: new window, stamps 1, allowed 1
    assertEquals(pass(), s.decide(1 + T + 1, 2, T)); // still in new window, allowed 2, no stamp
  }

  @Test
  void m2_windowAnchoredAtFirstEvent_lateSecondAllowedDoesNotExtendWindow() {
    ThrottleState s = new ThrottleState();
    // The window is anchored at the period's FIRST event, so it ends at first-event + T (= T),
    // NOT at the second allowed event + T. This is the model-A (duty cycle) vs model-B (fixed
    // period, Cribl-style) divergence point.
    assertEquals(pass(), s.decide(0, 2, T)); // window [0, T), allowed 1
    assertEquals(pass(), s.decide(T - 5_000, 2, T)); // still in [0, T), allowed 2 (arrives late)
    assertEquals(drop(), s.decide(T - 1_000, 2, T)); // over M within the window -> dropped 1
    // A duty cycle anchored at the 2nd event would suppress until 2T-5000 and DROP here; anchored
    // at the first event the window has ended at T, so this passes and opens the next period.
    assertEquals(passStamped(1), s.decide(T, 2, T));
  }

  @Test
  void noDrops_meansNoStampAfterPeriod() {
    ThrottleState s = new ThrottleState();
    assertEquals(pass(), s.decide(0, 1, T)); // starts drop-phase, but nothing dropped
    // next event after the period: exits with dropped==0 -> passes without a stamp
    assertEquals(pass(), s.decide(T, 1, T));
  }

  @Test
  void multiPeriodGap_reflectsOnlyTheOneDropPhase() {
    ThrottleState s = new ThrottleState();
    assertEquals(pass(), s.decide(0, 1, T));
    assertEquals(drop(), s.decide(1, 1, T));
    assertEquals(drop(), s.decide(2, 1, T)); // dropped 2
    // next event arrives several periods later: stamps exactly the 2 from that single drop-phase
    assertEquals(passStamped(2), s.decide(10 * T, 1, T));
  }
}
