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
 * Thrown by a {@link SimpleSinkCommand.Flusher} when records have already been handed to the target
 * system but the durability confirmation failed, leaving the commit state unknown.
 *
 * <p>This must NOT be treated as a normal per-record failure. {@link SimpleSinkCommand} converts an
 * ordinary thrown exception into one {@code ErrorOutput} per record (see {@code writeOneBatch}); in
 * the runner's non-DLQ path those are merely counted and then discarded, which would silently lose
 * records that may in fact have been written. Reporting a clean "complete failure" when the truth
 * is "we don't know" is worse than failing the node. So {@code writeOneBatch} rethrows this
 * exception instead, letting it propagate as a fatal node/job error.
 */
public class UnknownSinkCommitStateException extends RuntimeException {
  public UnknownSinkCommitStateException(String message, Throwable cause) {
    super(message, cause);
  }
}
