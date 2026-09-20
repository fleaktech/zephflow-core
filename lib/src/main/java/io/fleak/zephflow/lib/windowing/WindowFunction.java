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
import java.util.List;

/**
 * Defines what a window accumulates and what it emits when it fires. This is the seam where the
 * concrete reduction node (aggregation, sampling, suppress, ...) plugs its own logic into the
 * shared windowing substrate.
 *
 * @param <ACC> the per-key accumulator type
 */
public interface WindowFunction<ACC> {

  /** Creates a fresh accumulator for a newly opened window. */
  ACC init();

  /**
   * Folds one event into the accumulator. May return a new accumulator or mutate and return the
   * given one. Must not mutate the input {@code event} in place (it is shared with sibling
   * branches).
   */
  ACC add(ACC acc, RecordFleakData event);

  /**
   * Produces the output records for a firing window. Called once when the window fires; the window
   * is then discarded (tumbling semantics).
   *
   * @param key the group key of this window
   * @param acc the accumulated state
   * @return output records (may be empty)
   */
  List<RecordFleakData> emit(String key, ACC acc);
}
