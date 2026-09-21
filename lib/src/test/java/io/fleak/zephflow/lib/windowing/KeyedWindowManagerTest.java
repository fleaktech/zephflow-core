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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KeyedWindowManagerTest {

  /** Test window function: counts events per key and emits {@code {key, count}} when fired. */
  private static final WindowFunction<Long> COUNTING =
      new WindowFunction<>() {
        @Override
        public Long init() {
          return 0L;
        }

        @Override
        public Long add(Long acc, RecordFleakData event) {
          return acc + 1;
        }

        @Override
        public List<RecordFleakData> emit(String key, Long acc) {
          return List.of(rollup(key, acc));
        }
      };

  private static RecordFleakData rollup(String key, long count) {
    return (RecordFleakData) FleakData.wrap(Map.of("key", key, "count", count));
  }

  private static RecordFleakData event() {
    return (RecordFleakData) FleakData.wrap(Map.of("v", 1));
  }

  @Test
  void countTrigger_firesAndResetsAtThreshold() {
    KeyedWindowManager<Long> mgr =
        KeyedWindowManager.<Long>builder()
            .windowFunction(COUNTING)
            .trigger(WindowTrigger.count(3))
            .build();

    assertEquals(List.of(), mgr.onEvent("a", event(), 0));
    assertEquals(List.of(), mgr.onEvent("a", event(), 0));
    assertEquals(List.of(rollup("a", 3)), mgr.onEvent("a", event(), 0));
    assertEquals(0, mgr.openWindowCount(), "window should be discarded after firing");

    // A new event after firing opens a fresh window.
    assertEquals(List.of(), mgr.onEvent("a", event(), 0));
    assertEquals(1, mgr.openWindowCount());
  }

  @Test
  void timeTrigger_firesOnTickAfterPeriodElapses() {
    KeyedWindowManager<Long> mgr =
        KeyedWindowManager.<Long>builder()
            .windowFunction(COUNTING)
            .trigger(WindowTrigger.time(1000))
            .build();

    assertEquals(List.of(), mgr.onEvent("a", event(), 0));
    assertEquals(List.of(), mgr.onEvent("a", event(), 100));
    assertEquals(List.of(), mgr.onTick(500, false), "before the period elapses nothing fires");
    assertEquals(List.of(rollup("a", 2)), mgr.onTick(1000, false));
    assertEquals(0, mgr.openWindowCount());
  }

  @Test
  void finalFlush_emitsAllRemainingWindowsRegardlessOfTrigger() {
    KeyedWindowManager<Long> mgr =
        KeyedWindowManager.<Long>builder()
            .windowFunction(COUNTING)
            .trigger(WindowTrigger.time(Long.MAX_VALUE)) // never fires on its own
            .build();

    mgr.onEvent("a", event(), 0);
    mgr.onEvent("b", event(), 0);
    mgr.onEvent("b", event(), 0);

    assertEquals(List.of(), mgr.onTick(1000, false), "no window is due yet");
    // Access-ordered: "a" is least-recently-updated, so it is emitted before "b". Comparing the
    // full ordered list (not a Set) also guards against any window being emitted more than once.
    assertEquals(List.of(rollup("a", 1), rollup("b", 2)), mgr.onTick(2000, true));
    assertEquals(0, mgr.openWindowCount());
  }

  @Test
  void maxKeys_evictsAndEmitsLeastRecentlyUpdatedWindow() {
    KeyedWindowManager<Long> mgr =
        KeyedWindowManager.<Long>builder()
            .windowFunction(COUNTING)
            .trigger(WindowTrigger.count(Long.MAX_VALUE)) // never fires on its own
            .maxKeys(2)
            .build();

    assertEquals(List.of(), mgr.onEvent("a", event(), 1));
    assertEquals(List.of(), mgr.onEvent("b", event(), 2));
    // "c" pushes size to 3; the least-recently-updated window ("a") is flushed out.
    assertEquals(List.of(rollup("a", 1)), mgr.onEvent("c", event(), 3));
    assertEquals(2, mgr.openWindowCount());
  }

  @Test
  void idleTtl_flushesWindowsIdleBeyondTtlOnTick() {
    KeyedWindowManager<Long> mgr =
        KeyedWindowManager.<Long>builder()
            .windowFunction(COUNTING)
            .trigger(WindowTrigger.count(Long.MAX_VALUE)) // never fires on its own
            .idleTtlMs(1000)
            .build();

    mgr.onEvent("a", event(), 0);
    assertEquals(List.of(), mgr.onTick(500, false), "still within idle TTL");
    assertEquals(List.of(rollup("a", 1)), mgr.onTick(1000, false));
    assertEquals(0, mgr.openWindowCount());
  }

  @Test
  void countAndTime_anyTriggerFiresOnWhicheverComesFirst() {
    KeyedWindowManager<Long> mgr =
        KeyedWindowManager.<Long>builder()
            .windowFunction(COUNTING)
            .trigger(WindowTrigger.any(WindowTrigger.count(5), WindowTrigger.time(1000)))
            .build();

    // Count reaches 2 by t=200, well under 5, but the time trigger fires on the tick at t=1000.
    mgr.onEvent("a", event(), 0);
    mgr.onEvent("a", event(), 200);
    assertEquals(List.of(rollup("a", 2)), mgr.onTick(1000, false));

    // Now the count trigger wins first: 5 events before any tick.
    for (int i = 0; i < 4; i++) {
      assertEquals(List.of(), mgr.onEvent("b", event(), 1000));
    }
    assertEquals(List.of(rollup("b", 5)), mgr.onEvent("b", event(), 1000));
  }

  @Test
  void emitThrowingForOneKey_doesNotDropOtherKeysOutput() {
    WindowFunction<Long> throwsOnBad =
        new WindowFunction<>() {
          @Override
          public Long init() {
            return 0L;
          }

          @Override
          public Long add(Long acc, RecordFleakData event) {
            return acc + 1;
          }

          @Override
          public List<RecordFleakData> emit(String key, Long acc) {
            if ("bad".equals(key)) {
              throw new RuntimeException("boom");
            }
            return List.of(rollup(key, acc));
          }
        };
    KeyedWindowManager<Long> mgr =
        KeyedWindowManager.<Long>builder()
            .windowFunction(throwsOnBad)
            .trigger(WindowTrigger.time(Long.MAX_VALUE)) // only finalFlush fires
            .build();

    mgr.onEvent("good", event(), 0);
    mgr.onEvent("bad", event(), 0);

    // "bad" throws in emit but is caught; "good" is still emitted and both windows are cleared.
    assertEquals(List.of(rollup("good", 1)), mgr.onTick(1000, true));
    assertEquals(0, mgr.openWindowCount());
  }

  @Test
  void maxKeys_reTouchedKeyIsRescuedFromEviction() {
    // Characterizes access-order LRU: re-touching an existing key must move it to
    // most-recently-used
    // so the genuinely-oldest-touched key is evicted, not the oldest-created one.
    KeyedWindowManager<Long> mgr =
        KeyedWindowManager.<Long>builder()
            .windowFunction(COUNTING)
            .trigger(WindowTrigger.count(Long.MAX_VALUE)) // never fires on its own
            .maxKeys(2)
            .build();

    assertEquals(List.of(), mgr.onEvent("a", event(), 1));
    assertEquals(List.of(), mgr.onEvent("b", event(), 2));
    assertEquals(
        List.of(), mgr.onEvent("a", event(), 3)); // re-touch a -> a is now MRU, b is eldest
    // "c" pushes size to 3; eldest is "b" (a was rescued by the re-touch), so "b" is evicted.
    assertEquals(List.of(rollup("b", 1)), mgr.onEvent("c", event(), 4));
    assertEquals(2, mgr.openWindowCount());
  }

  @Test
  void triggerFactories_rejectNonPositiveThresholds() {
    assertThrows(IllegalArgumentException.class, () -> WindowTrigger.count(0));
    assertThrows(IllegalArgumentException.class, () -> WindowTrigger.count(-1));
    assertThrows(IllegalArgumentException.class, () -> WindowTrigger.time(0));
    assertThrows(IllegalArgumentException.class, () -> WindowTrigger.time(-5));
  }
}
