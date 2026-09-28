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
 * A keyed-stateful command that holds pending output which must be emitted when its input ends: a
 * bounded input declared finished by the caller (test run, inspection), a finite source reaching
 * its end, or a graceful pipeline shutdown. The runner calls {@link #flushAtEndOfInput} on these
 * nodes in topological order and routes the output downstream like {@code process} output.
 *
 * <p>After the call the command holds no pending output, so a second end of input emits nothing.
 */
public interface EndOfInputFlushable extends KeyedStatefulCommand {

  /**
   * Emits every pending output record.
   *
   * @param callingUser the calling user id, matching the value passed to {@link
   *     ScalarCommand#process}
   * @param context the command's execution context
   * @return output records to route downstream; empty when nothing is pending
   */
  List<RecordFleakData> flushAtEndOfInput(String callingUser, ExecutionContext context);
}
