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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The shared per-key state primitive underneath the reduction substrate: one mutable state object
 * per group key, ordered by access so the eldest entry is the genuine least-recently-touched one,
 * with a size cap and idle eviction. Reduction commands (aggregation windows, throttle, ...) build
 * on this; it carries no emit policy — callers decide what to do with evicted state.
 *
 * <p><b>Not thread-safe.</b> Callers (the runner) serialize access via the pipeline lock, so this
 * class holds no internal locks.
 *
 * @param <S> the per-key state type
 */
public class KeyedStateStore<S> {

  /** A key's state plus its processing-time bookkeeping (epoch millis). */
  public static final class Entry<S> {
    private final S state;
    private final long createdAtMs;
    private long lastUpdatedMs;

    private Entry(S state, long nowMs) {
      this.state = state;
      this.createdAtMs = nowMs;
      this.lastUpdatedMs = nowMs;
    }

    public S state() {
      return state;
    }

    public long createdAtMs() {
      return createdAtMs;
    }

    public long lastUpdatedMs() {
      return lastUpdatedMs;
    }
  }

  /** An evicted key together with its state, returned so the caller can emit or drop it. */
  public record Evicted<S>(String key, S state) {}

  private final Map<String, Entry<S>> entries = new LinkedHashMap<>(16, 0.75f, true);

  /**
   * Returns the entry for {@code key}, creating it via {@code factory} if absent. Either way the
   * key is moved to most-recently-used and its {@code lastUpdatedMs} is set to {@code nowMs}.
   */
  public Entry<S> getOrCreate(String key, long nowMs, Supplier<S> factory) {
    Entry<S> e = entries.get(key); // access-order touch for an existing key
    if (e == null) {
      e = new Entry<>(factory.get(), nowMs);
      entries.put(key, e);
    }
    e.lastUpdatedMs = nowMs;
    return e;
  }

  public void remove(String key) {
    entries.remove(key);
  }

  public int size() {
    return entries.size();
  }

  /**
   * Live view of the entries in access order (eldest first) for iteration. Iterating (and removing
   * via the iterator) does NOT change the access order of surviving entries.
   */
  public Set<Map.Entry<String, Entry<S>>> entryView() {
    return entries.entrySet();
  }

  /**
   * Evicts the least-recently-touched entries until at most {@code maxKeys} remain. {@code maxKeys
   * <= 0} means unlimited (no-op). Returns the evicted entries (eldest first) for the caller to
   * handle.
   */
  public List<Evicted<S>> evictLruBeyond(int maxKeys) {
    if (maxKeys <= 0 || entries.size() <= maxKeys) {
      return List.of();
    }
    List<Evicted<S>> evicted = new ArrayList<>();
    Iterator<Map.Entry<String, Entry<S>>> it = entries.entrySet().iterator();
    while (entries.size() > maxKeys && it.hasNext()) {
      Map.Entry<String, Entry<S>> eldest = it.next();
      String key = eldest.getKey();
      S state = eldest.getValue().state;
      it.remove();
      evicted.add(new Evicted<>(key, state));
    }
    return evicted;
  }

  /**
   * Evicts entries not touched within {@code idleTtlMs}. {@code idleTtlMs <= 0} disables idle
   * eviction (no-op). Returns the evicted entries for the caller to handle.
   */
  public List<Evicted<S>> evictIdle(long nowMs, long idleTtlMs) {
    if (idleTtlMs <= 0) {
      return List.of();
    }
    List<Evicted<S>> evicted = new ArrayList<>();
    Iterator<Map.Entry<String, Entry<S>>> it = entries.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Entry<S>> e = it.next();
      if (nowMs - e.getValue().lastUpdatedMs >= idleTtlMs) {
        String key = e.getKey();
        S state = e.getValue().state;
        it.remove();
        evicted.add(new Evicted<>(key, state));
      }
    }
    return evicted;
  }
}
