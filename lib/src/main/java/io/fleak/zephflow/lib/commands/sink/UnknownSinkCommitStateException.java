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
 * <p>It is handled like any other flush failure by {@link SimpleSinkCommand}: with
 * store-and-forward enrolled the batch is buffered to local storage (the classifier walks the cause
 * chain and treats the underlying network signal as transient), giving at-least-once via replay;
 * otherwise it is converted to per-record {@code ErrorOutput}s that go to the DLQ (at-least-once)
 * or are dropped in the plain non-DLQ path (at-most-once). It no longer fails the whole job — that
 * would couple unrelated sink branches, breaking per-node fault isolation.
 */
public class UnknownSinkCommitStateException extends RuntimeException {
  public UnknownSinkCommitStateException(String message, Throwable cause) {
    super(message, cause);
  }
}
