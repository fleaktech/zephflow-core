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

import java.util.List;

/**
 * Tracks how much of the latest fetch a fetcher has committed. The source loop fetches again only
 * after it has handled every record of the previous fetch, so whatever a new fetch leaves
 * uncommitted from the previous one is handled and may be committed.
 */
public final class FetchProgress<T> {

  private List<T> fetched = List.of();
  private int committed;

  /** Starts a fetch and returns the uncommitted records of the previous one. */
  public List<T> next(List<T> records) {
    List<T> rest = List.copyOf(fetched.subList(committed, fetched.size()));
    fetched = List.copyOf(records);
    committed = 0;
    return rest;
  }

  /** Returns the records of the latest fetch, up to {@code handledRecords}, not returned before. */
  public List<T> through(int handledRecords) {
    int end = Math.min(handledRecords, fetched.size());
    if (end <= committed) {
      return List.of();
    }
    List<T> records = List.copyOf(fetched.subList(committed, end));
    committed = end;
    return records;
  }

  /** Returns every record of the latest fetch not returned before. */
  public List<T> rest() {
    return through(fetched.size());
  }
}
