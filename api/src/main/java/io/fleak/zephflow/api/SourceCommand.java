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

/** Created by bolei on 9/23/24 */
public abstract class SourceCommand extends OperatorCommand {

  protected SourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  public abstract void execute(String callingUser, SourceEventAcceptor sourceEventAcceptor)
      throws Exception;

  public abstract SourceType sourceType();

  /**
   * Asks a running {@link #execute} to stop taking input and return, which ends the pipeline with
   * its end-of-input flush. Unlike {@link #terminate}, it releases nothing, so it is safe to call
   * from another thread while {@code execute} is fetching.
   *
   * @return whether this source will stop; the default cannot be stopped this way
   */
  public boolean requestStop() {
    return false;
  }

  /**
   * Whether {@link #execute} calls {@link SourceEventAcceptor#beforeCommit} before every commit.
   */
  public boolean confirmsDeliveryBeforeCommit() {
    return false;
  }

  public enum SourceType {
    BATCH,
    STREAMING
  }
}
