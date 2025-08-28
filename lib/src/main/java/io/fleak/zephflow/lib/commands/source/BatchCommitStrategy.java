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
 * Commit strategy that commits after processing a batch of records or after a specified time
 * interval. This provides better performance for high-throughput sources at the cost of potential
 * duplicate processing in case of failures.
 */
public record BatchCommitStrategy(int batchSize, long intervalMs) implements CommitStrategy {

  /**
   * Creates a batch commit strategy.
   *
   * @param batchSize maximum number of records before committing (0 for no limit)
   * @param intervalMs maximum time in ms before committing (0 for no time limit)
   */
  public BatchCommitStrategy {}

  /**
   * Creates a batch commit strategy with only batch size limit.
   *
   * @param batchSize maximum number of records before committing
   */
  public static BatchCommitStrategy ofBatchSize(int batchSize) {
    return new BatchCommitStrategy(batchSize, 0);
  }

  /**
   * Creates a batch commit strategy with only time interval limit.
   *
   * @param intervalMs maximum time in ms before committing
   */
  public static BatchCommitStrategy ofInterval(long intervalMs) {
    return new BatchCommitStrategy(0, intervalMs);
  }

  /**
   * Creates a batch commit strategy optimized for Kafka. Commits every 1000 records or 5 seconds,
   * whichever comes first.
   */
  public static BatchCommitStrategy forKafka() {
    return new BatchCommitStrategy(1000, 5000);
  }

  @Override
  public CommitMode getCommitMode() {
    return CommitMode.BATCH;
  }

  @Override
  public int getCommitBatchSize() {
    return batchSize;
  }

  @Override
  public long getCommitIntervalMs() {
    return intervalMs;
  }
}
