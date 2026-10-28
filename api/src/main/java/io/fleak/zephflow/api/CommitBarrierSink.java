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
 * A sink that can return from a write before the target system confirms it, provided the source
 * waits for those confirmations before committing its read position.
 */
public interface CommitBarrierSink {

  /** Allows writes to return before confirmation. Called before the sink is initialized. */
  void deferDeliveryConfirmation();

  /**
   * Waits for every write made so far to be confirmed or rejected.
   *
   * @throws Exception if some write may not have been delivered and the source must not commit
   */
  void awaitDeliveryConfirmation() throws Exception;
}
