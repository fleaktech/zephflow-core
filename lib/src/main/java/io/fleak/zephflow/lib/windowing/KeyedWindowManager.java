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

import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * The reusable keyed-windowing substrate: one in-memory window per group key, a pluggable {@link
 * WindowTrigger} (time and/or count), and per-key state bounded by a size cap and idle eviction.
 * Reduction nodes (aggregation, sampling, suppress, ...) build on this rather than re-implementing
 * windowing.
 *
 * <p>Windows are <b>tumbling</b>: a window is discarded when it fires, and the next event for that
 * key opens a fresh one.
 *
 * <p>Emission carries <b>no cross-key ordering guarantee</b>. Within a single {@link #onTick} /
 * {@link #enforceMaxKeys} pass windows are visited in least-recently-updated order, but callers
 * must not depend on the relative order of different keys' output.
 *
 * <p><b>Not thread-safe.</b> The runner serializes {@link #onEvent} (from {@code process}) against
 * {@link #onTick} (from the flush thread) with its pipeline lock, so this class deliberately holds
 * no internal locks.
 *
 * @param <ACC> the per-key accumulator type
 */
@Slf4j
public class KeyedWindowManager<ACC> {

  private final WindowFunction<ACC> windowFunction;
  private final WindowTrigger trigger;
  private final int maxKeys; // <= 0 means unlimited
  private final long idleTtlMs; // <= 0 means disabled
  // access-order so the eldest entry is the genuine least-recently-updated window (deterministic
  // eviction even when several events share a coarse System.currentTimeMillis() timestamp).
  private final Map<String, Window<ACC>> windows = new LinkedHashMap<>(16, 0.75f, true);

  @Builder
  public KeyedWindowManager(
      WindowFunction<ACC> windowFunction, WindowTrigger trigger, int maxKeys, long idleTtlMs) {
    this.windowFunction = Objects.requireNonNull(windowFunction, "windowFunction");
    this.trigger = Objects.requireNonNull(trigger, "trigger");
    this.maxKeys = maxKeys;
    this.idleTtlMs = idleTtlMs;
  }

  /**
   * Folds one event into its key's window and returns any output produced if the window fires
   * (count trigger) or if enforcing the size cap evicts a window.
   */
  public List<RecordFleakData> onEvent(String key, RecordFleakData event, long nowMs) {
    Window<ACC> w = windows.get(key);
    if (w == null) {
      w = new Window<>(windowFunction.init(), nowMs);
      windows.put(key, w);
    }
    w.acc = windowFunction.add(w.acc, event);
    w.count++;
    w.lastUpdatedMs = nowMs;

    List<RecordFleakData> out = new ArrayList<>();
    if (trigger.shouldFire(w.meta(), nowMs)) {
      windows.remove(key);
      out.addAll(safeEmit(key, w.acc));
    }
    out.addAll(enforceMaxKeys());
    return out;
  }

  /**
   * Fires all windows that are due on a processing-time tick, or every remaining window when {@code
   * finalFlush} is set (pipeline shutdown). Also flushes out windows idle beyond the configured
   * TTL.
   */
  public List<RecordFleakData> onTick(long nowMs, boolean finalFlush) {
    List<RecordFleakData> out = new ArrayList<>();
    Iterator<Map.Entry<String, Window<ACC>>> it = windows.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Window<ACC>> e = it.next();
      Window<ACC> w = e.getValue();
      boolean idleExpired = idleTtlMs > 0 && (nowMs - w.lastUpdatedMs) >= idleTtlMs;
      if (finalFlush || idleExpired || trigger.shouldFire(w.meta(), nowMs)) {
        String key = e.getKey();
        it.remove();
        out.addAll(safeEmit(key, w.acc));
      }
    }
    return out;
  }

  public int openWindowCount() {
    return windows.size();
  }

  /**
   * Keeps the number of open windows within {@link #maxKeys} by flushing out the least-recently
   * updated windows (the eldest entries in the access-ordered map). Eviction emits (rather than
   * drops) so no accumulated data is lost.
   */
  private List<RecordFleakData> enforceMaxKeys() {
    if (maxKeys <= 0 || windows.size() <= maxKeys) {
      return List.of();
    }
    List<RecordFleakData> out = new ArrayList<>();
    Iterator<Map.Entry<String, Window<ACC>>> it = windows.entrySet().iterator();
    while (windows.size() > maxKeys && it.hasNext()) {
      Map.Entry<String, Window<ACC>> eldest = it.next();
      String key = eldest.getKey();
      Window<ACC> w = eldest.getValue();
      it.remove();
      out.addAll(safeEmit(key, w.acc));
    }
    return out;
  }

  private List<RecordFleakData> safeEmit(String key, ACC acc) {
    try {
      return windowFunction.emit(key, acc);
    } catch (Exception e) {
      log.error("window emit failed for key {}; dropping this window's output", key, e);
      return List.of();
    }
  }

  private static final class Window<A> {
    private A acc;
    private long count;
    private final long createdAtMs;
    private long lastUpdatedMs;

    Window(A acc, long createdAtMs) {
      this.acc = acc;
      this.count = 0L;
      this.createdAtMs = createdAtMs;
      this.lastUpdatedMs = createdAtMs;
    }

    WindowMeta meta() {
      return new WindowMeta(count, createdAtMs, lastUpdatedMs);
    }
  }
}
