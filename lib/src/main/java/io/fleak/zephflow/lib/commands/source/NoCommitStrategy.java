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
package io.fleak.zephflow.lib.commands.source;

/**
 * Commit strategy that never commits manually. This delegates all commit responsibility to the
 * underlying source (e.g., Kafka auto-commit, or sources that don't need commits like file
 * sources).
 */
public class NoCommitStrategy implements CommitStrategy {

  public static final NoCommitStrategy INSTANCE = new NoCommitStrategy();

  @Override
  public CommitMode getCommitMode() {
    return CommitMode.NONE;
  }

  @Override
  public int getCommitBatchSize() {
    return 0;
  }

  @Override
  public long getCommitIntervalMs() {
    return 0;
  }

  @Override
  public boolean shouldCommitNow(int recordCount, long timeSinceLastCommit) {
    return false;
  }
}
