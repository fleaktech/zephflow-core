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
package io.fleak.zephflow.lib.sql.exec.functions;

import java.util.List;

/** */
public interface AggregateFunction<T, R> {

  /** The accumulation state */
  T initState();

  /** For each row this method is called */
  T update(T state, List<Object> arguments);

  /** To support parralization, we can combine two different states from different threads. */
  T combine(T state1, T state2);

  /** Produce the final result */
  R getResult(T state);
}
