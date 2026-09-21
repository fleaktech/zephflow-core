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

import io.fleak.zephflow.lib.windowing.KeyedStateStore.Evicted;
import java.util.List;
import org.junit.jupiter.api.Test;

class KeyedStateStoreTest {

  private static KeyedStateStore<String> store() {
    return new KeyedStateStore<>();
  }

  @Test
  void getOrCreate_createsThenReuses_andReTouchMovesToMru() {
    KeyedStateStore<String> s = store();
    assertEquals("a", s.getOrCreate("a", 1, () -> "a").state());
    s.getOrCreate("b", 2, () -> "b");
    // re-touch "a": factory not invoked, "a" becomes MRU, "b" is now eldest
    assertEquals("a", s.getOrCreate("a", 3, () -> "SHOULD_NOT_BE_USED").state());
    assertEquals(2, s.size());

    assertEquals(List.of(new Evicted<>("b", "b")), s.evictLruBeyond(1));
    assertEquals(1, s.size());
  }

  @Test
  void evictLruBeyond_evictsEldestFirst_andNoOpWhenUnlimitedOrWithinCap() {
    KeyedStateStore<String> s = store();
    s.getOrCreate("a", 1, () -> "a");
    s.getOrCreate("b", 2, () -> "b");
    s.getOrCreate("c", 3, () -> "c");

    assertEquals(List.of(), s.evictLruBeyond(0), "maxKeys<=0 is unlimited (no-op)");
    assertEquals(List.of(), s.evictLruBeyond(5), "within cap (no-op)");
    assertEquals(List.of(new Evicted<>("a", "a"), new Evicted<>("b", "b")), s.evictLruBeyond(1));
    assertEquals(1, s.size());
  }

  @Test
  void evictIdle_evictsOnlyEntriesIdleBeyondTtl() {
    KeyedStateStore<String> s = store();
    s.getOrCreate("a", 0, () -> "a");
    s.getOrCreate("b", 700, () -> "b");

    assertEquals(List.of(), s.evictIdle(1000, 0), "ttl<=0 disables idle eviction");
    // at now=1000: a idle 1000>=500 -> evicted; b idle 300<500 -> kept
    assertEquals(List.of(new Evicted<>("a", "a")), s.evictIdle(1000, 500));
    assertEquals(1, s.size());
  }

  @Test
  void iterationDoesNotPerturbAccessOrder() {
    KeyedStateStore<String> s = store();
    s.getOrCreate("a", 1, () -> "a");
    s.getOrCreate("b", 2, () -> "b");
    s.getOrCreate("c", 3, () -> "c");

    // evictIdle iterates the whole entry view (no-op here) — must not reorder survivors
    assertEquals(List.of(), s.evictIdle(5, 1000));
    // eldest is still "a"
    assertEquals(List.of(new Evicted<>("a", "a")), s.evictLruBeyond(2));
  }
}
