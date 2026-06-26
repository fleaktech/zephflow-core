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

import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.Closeable;
import java.util.List;

/**
 * Store-and-forward collaborator for a sink. A sink holds a reference to one of these; when a flush
 * fails because the remote is unreachable, the records are persisted to local durable storage and
 * replayed once connectivity is restored, instead of being dropped.
 *
 * <p>This is composition + Null Object: every sink gets a {@link #noop()} by default, which does
 * nothing and short-circuits both {@link #isBuffering()} and {@link #shouldBuffer(Throwable)} to
 * {@code false}, so non-enrolled sinks keep their exact current behavior. A sink opts in by
 * injecting a real implementation (e.g. {@link ChronicleStoreForward}).
 *
 * <p>The replay unit is the raw {@link RecordFleakData}, so the on-disk format is identical across
 * all sinks and replay re-runs the normal preprocess+flush path (re-encoding against the current
 * schema).
 */
public interface SinkStoreForward extends Closeable {

  /**
   * Whether the sink is currently in an outage and new records should be routed straight to disk.
   */
  boolean isBuffering();

  /**
   * Whether the given flush failure is a transient connectivity failure that should be buffered.
   */
  boolean shouldBuffer(Throwable t);

  /**
   * Persists the given raw records to local storage (oldest-first) and flips into buffering mode.
   *
   * @return the number of records stored from the front of the list; any remainder was dropped
   *     because the local store hit its size cap.
   */
  int offer(List<RecordFleakData> records);

  /**
   * Starts the background forwarder. {@code target} delivers a chunk of buffered records back to
   * the remote; it must throw on failure so the forwarder can keep the records and retry.
   */
  void start(ReplayTarget target);

  /** Re-delivers a chunk of buffered records to the remote sink. */
  @FunctionalInterface
  interface ReplayTarget {
    void deliver(List<RecordFleakData> records) throws Exception;
  }

  /** The do-nothing Null Object injected into every sink that has not enrolled. */
  static SinkStoreForward noop() {
    return NoOpSinkStoreForward.INSTANCE;
  }
}
