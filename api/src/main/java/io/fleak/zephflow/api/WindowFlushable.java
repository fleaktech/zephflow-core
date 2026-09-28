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
package io.fleak.zephflow.api;

import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.List;

/**
 * Opt-in hook for commands that accumulate state across events and must be able to emit output at a
 * window boundary <b>even when no new input arrives</b>.
 *
 * <p>The default per-event model ({@link ScalarCommand#process}) only produces output in response
 * to an input event. A time-triggered window has no such event at its boundary, so the runner
 * periodically calls {@link #flush} on a background thread to let the command emit due windows.
 *
 * <p>Only commands that need time-based triggering implement this. The runner detects implementors
 * and only then starts its flush scheduler, so non-windowed pipelines are entirely unaffected.
 *
 * <p><b>Concurrency:</b> the runner serializes {@code flush} against {@code process} for the same
 * command instance (see the runner's pipeline lock), so implementations do not need internal
 * synchronization between the two. Note however that {@code flush} runs on a background flush
 * thread, and its output is routed downstream on that same thread — so sink and intermediate
 * commands placed downstream of a windowed node may be invoked from either the source thread or the
 * flush thread (never concurrently). Such downstream commands must not pin resources to their
 * creating thread (e.g. thread-confined connections or {@link ThreadLocal} buffers).
 *
 * <p><b>No in-place mutation:</b> the {@link RecordFleakData} records returned here flow downstream
 * exactly like {@code process} output and must obey the same contract — do not mutate shared input
 * records in place.
 */
public interface WindowFlushable extends EndOfInputFlushable {

  /** At end of input every remaining window fires, exactly like a final flush. */
  @Override
  default List<RecordFleakData> flushAtEndOfInput(String callingUser, ExecutionContext context) {
    return flush(callingUser, context, true);
  }

  /**
   * Emits output for windows that are due.
   *
   * @param callingUser the calling user id, matching the value passed to {@link
   *     ScalarCommand#process}
   * @param context the command's execution context (holds the per-instance window state)
   * @param finalFlush when {@code true} (pipeline shutdown), emit <b>all</b> remaining windows
   *     regardless of their trigger; when {@code false} (periodic tick), emit only windows whose
   *     time trigger has fired
   * @return output records to route downstream; empty when nothing is due
   */
  List<RecordFleakData> flush(String callingUser, ExecutionContext context, boolean finalFlush);
}
