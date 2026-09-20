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

/**
 * Read-only bookkeeping for a single key's window, passed to a {@link WindowTrigger} to decide
 * whether the window should fire. All timestamps are processing-time epoch millis.
 *
 * @param count number of events folded into the window since it was created
 * @param createdAtMs when the window was (re)created, i.e. when its first event arrived
 * @param lastUpdatedMs when the most recent event was folded in
 */
public record WindowMeta(long count, long createdAtMs, long lastUpdatedMs) {}
