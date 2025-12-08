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

import java.io.Closeable;
import java.util.List;

/** Created by bolei on 11/5/24 */
public interface Fetcher<T> extends Closeable {

  List<T> fetch();

  /**
   * Returns true if this fetcher has exhausted its data source and will never produce more data.
   * Once this returns true, it should continue returning true.
   *
   * <p>Infinite sources (Kafka, Kinesis) should always return false. Finite sources (File, Splunk,
   * paginated APIs) should return true when exhausted.
   *
   * @return true if no more data will ever be available
   */
  default boolean isExhausted() {
    return false;
  }

  default Fetcher.Committer committer() {
    return null;
  }

  /**
   * Returns the commit strategy for this fetcher. The default strategy commits after each record to
   * maintain backward compatibility.
   *
   * @return the commit strategy to use
   */
  default CommitStrategy commitStrategy() {
    return PerRecordCommitStrategy.INSTANCE;
  }

  @FunctionalInterface
  interface Committer {
    void commit() throws Exception;
  }
}
