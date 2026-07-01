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
package io.fleak.zephflow.lib.commands.sink;

/**
 * Decides whether a flush failure is a transient connectivity failure (which store-and-forward
 * should buffer and retry) rather than a permanent one (bad record, auth, schema). This is
 * per-sink: each SDK surfaces connectivity differently, so the owning sink supplies the rule.
 *
 * <p>Implementations should default to {@code false} for anything they don't recognize, so an
 * unknown failure stays on the normal error path rather than being hoarded forever.
 */
@FunctionalInterface
public interface ConnectionFailureClassifier {
  boolean isConnectionFailure(Throwable t);
}
