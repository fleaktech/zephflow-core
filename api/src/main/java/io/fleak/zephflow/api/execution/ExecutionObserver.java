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
import java.util.List;

/**
 * Synchronous observations of one run. Callbacks finish before processing continues. Record lists
 * are valid for the duration of the callback; implementations must copy or serialize before return.
 * Callback failures abort execution, including callbacks that publish delivery acknowledgements.
 */
public interface ExecutionObserver {
  enum Side {
    INPUT,
    OUTPUT
  }

  enum Phase {
    INIT,
    PROCESS,
    END_OF_INPUT,
    BACKGROUND,
    DISPOSE
  }

  record Invocation(long id, String nodeId, String upstreamNodeId, Phase phase, String bindingId) {}

  void invocationStarted(long sequence, Invocation invocation);

  void records(long sequence, Invocation invocation, Side side, List<RecordFleakData> records);

  void recordErrors(long sequence, Invocation invocation, List<ErrorOutput> errors);

  void invocationFailed(long sequence, Invocation invocation, Throwable failure);

  void invocationFinished(
      long sequence, Invocation invocation, long inputCount, long outputCount, boolean complete);

  void effectStarted(long sequence, Invocation invocation, long effectId);

  void effectFinished(long sequence, Invocation invocation, long effectId, EffectOutcome outcome);
}
