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

/**
 * Marker for commands that keep per-key in-memory reduction state (windowed aggregation, throttle,
 * sample, ...). Such state assumes a long-lived streaming pipeline: it accumulates across events
 * and, on a reused request/response runner, would leak across independent requests. {@code
 * DagRunnerService#createForApiBackend} rejects these commands; the one-shot test-run runner
 * ({@code createForTestRun}) allows them.
 *
 * <p>{@link EndOfInputFlushable} extends this for commands holding pending output at end of input
 * (sampling, and {@link WindowFlushable} windowed commands); throttle implements this directly.
 */
public interface KeyedStatefulCommand {}
