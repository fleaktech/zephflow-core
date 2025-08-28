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
 * Defines the commit strategy for a source fetcher, allowing different sources to optimize their
 * commit behavior independently.
 */
public interface CommitStrategy {

  /** The different modes of committing. */
  enum CommitMode {
    /** Commit after processing each individual record. */
    PER_RECORD,
    /** Commit after processing a batch of records. */
    BATCH,
    /** No automatic commits - delegated to external mechanisms. */
    NONE
  }

  /**
   * Returns the commit mode for this strategy.
   *
   * @return the commit mode
   */
  CommitMode getCommitMode();

  /**
   * The maximum number of records to process before forcing a commit. Only relevant if
   * shouldCommitAfterBatch() returns true.
   *
   * @return maximum batch size, or 0 for no limit
   */
  int getCommitBatchSize();

  /**
   * The maximum time in milliseconds to wait before forcing a commit. Only relevant if
   * shouldCommitAfterBatch() returns true.
   *
   * @return maximum interval in ms, or 0 for no time limit
   */
  long getCommitIntervalMs();

  /**
   * Called when a batch of records starts processing. Can be used to initialize timing or counting.
   */
  default void onBatchStart() {}

  /**
   * Called when a record has been successfully processed.
   *
   * @param recordCount the number of records processed so far in this batch
   * @param timeSinceLastCommit milliseconds since the last commit
   * @return true if a commit should happen now
   */
  default boolean shouldCommitNow(int recordCount, long timeSinceLastCommit) {
    CommitMode mode = getCommitMode();

    if (mode == CommitMode.PER_RECORD) {
      return true;
    }

    if (mode == CommitMode.NONE) {
      return false;
    }

    // BATCH mode
    int batchSize = getCommitBatchSize();
    long intervalMs = getCommitIntervalMs();

    return (batchSize > 0 && recordCount >= batchSize)
        || (intervalMs > 0 && timeSinceLastCommit >= intervalMs);
  }
}
