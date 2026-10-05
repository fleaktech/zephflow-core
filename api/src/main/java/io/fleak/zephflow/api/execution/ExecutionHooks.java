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

import java.util.Objects;

/** Runtime-only hooks passed to an operator and its resources; never serialized with a DAG. */
public record ExecutionHooks(
    ExecutionControl control, Effects effects, BackgroundWork backgroundWork) {
  public ExecutionHooks {
    Objects.requireNonNull(control);
    Objects.requireNonNull(effects);
    Objects.requireNonNull(backgroundWork);
  }

  /** One adapter operation, with its receipt recorded before the next operation may start. */
  public interface Effects {
    long started();

    void finished(long effectId, EffectOutcome outcome);
  }

  /** Runs existing background work under the same access, stop, and observation contract. */
  @FunctionalInterface
  public interface BackgroundWork {
    void run(Runnable work);
  }
}
