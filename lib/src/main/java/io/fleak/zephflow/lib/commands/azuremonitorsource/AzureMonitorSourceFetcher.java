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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import java.util.List;
import java.util.Map;

public class AzureMonitorSourceFetcher implements Fetcher<Map<String, String>> {

  private final String kqlQuery;
  private final int batchSize;
  private final AzureMonitorQueryClient queryClient;

  private List<Map<String, String>> allRows;
  private int offset = 0;

  public AzureMonitorSourceFetcher(
      String kqlQuery, int batchSize, AzureMonitorQueryClient queryClient) {
    this.kqlQuery = kqlQuery;
    this.batchSize = batchSize;
    this.queryClient = queryClient;
  }

  @Override
  public List<Map<String, String>> fetch() {
    if (allRows == null) {
      try {
        allRows = queryClient.executeQuery(kqlQuery);
      } catch (Exception e) {
        throw new RuntimeException("Failed to execute Azure Monitor query", e);
      }
    }
    if (isExhausted()) {
      return List.of();
    }
    int end = Math.min(offset + batchSize, allRows.size());
    List<Map<String, String>> batch = List.copyOf(allRows.subList(offset, end));
    offset = end;
    return batch;
  }

  @Override
  public boolean isExhausted() {
    return allRows != null && offset >= allRows.size();
  }

  @Override
  public CommitStrategy commitStrategy() {
    return NoCommitStrategy.INSTANCE;
  }

  @Override
  public void close() {}
}
