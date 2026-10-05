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
package io.fleak.zephflow.runner;

import io.fleak.zephflow.api.execution.ExecutionObserver;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.List;
import java.util.Objects;

/** One full logical input population. Storage pages must not become separate bindings. */
public record BoundInput(
    String bindingId, String nodeId, ExecutionObserver.Side side, List<RecordFleakData> records) {
  public BoundInput {
    Objects.requireNonNull(bindingId);
    Objects.requireNonNull(nodeId);
    Objects.requireNonNull(side);
    records = List.copyOf(records);
  }
}
