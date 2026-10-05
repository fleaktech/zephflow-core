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
package io.fleak.zephflow.api.execution;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.ArrayList;
import java.util.List;

/**
 * Carries already-produced progress to the observer when cooperative stopping interrupts a batch.
 */
public final class ExecutionProgressStoppedException extends ExecutionStoppedException {
  private final List<RecordFleakData> output;
  private final List<ErrorOutput> errors;

  public ExecutionProgressStoppedException(
      ExecutionStoppedException cause, List<RecordFleakData> output, List<ErrorOutput> errors) {
    super(cause.getMessage(), cause);
    var retainedOutput = new ArrayList<>(output);
    var retainedErrors = new ArrayList<>(errors);
    if (cause instanceof ExecutionProgressStoppedException partial) {
      retainedOutput.addAll(partial.output());
      retainedErrors.addAll(partial.errors());
    }
    this.output = List.copyOf(retainedOutput);
    this.errors = List.copyOf(retainedErrors);
  }

  public List<RecordFleakData> output() {
    return output;
  }

  public List<ErrorOutput> errors() {
    return errors;
  }
}
