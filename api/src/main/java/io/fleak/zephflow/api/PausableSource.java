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
 * A source a supervisor can pause and resume while {@link SourceCommand#execute} runs. Pausing
 * stops reading, commits what has been handled and gives up any shared ownership (partitions,
 * shards, leases) so other consumers take over; resuming reads again. All methods are safe to call
 * from another thread.
 */
public interface PausableSource {

  /** Whether this source can pause without losing input. */
  default boolean supportsPause() {
    return true;
  }

  void requestPause();

  void requestResume();

  /** True once a requested pause has taken effect: reading stopped and progress committed. */
  boolean isPaused();
}
