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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AzureMonitorSourceFetcherTest {

  private static List<Map<String, String>> rows(int count) {
    List<Map<String, String>> list = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      list.add(Map.of("idx", String.valueOf(i)));
    }
    return list;
  }

  @Test
  void singleBatch_allRowsReturnedInOneFetch_thenExhausted() throws Exception {
    AzureMonitorQueryClient mockClient = mock(AzureMonitorQueryClient.class);
    when(mockClient.executeQuery("query")).thenReturn(rows(3));
    AzureMonitorSourceFetcher fetcher = new AzureMonitorSourceFetcher("query", 10, mockClient);

    assertFalse(fetcher.isExhausted());

    List<Map<String, String>> batch = fetcher.fetch();
    assertEquals(3, batch.size());
    assertEquals("0", batch.get(0).get("idx"));
    assertEquals("2", batch.get(2).get("idx"));

    assertTrue(fetcher.isExhausted());
    verify(mockClient, times(1)).executeQuery("query");
  }

  @Test
  void multiBatch_rowsSplitAcrossMultipleFetchCalls() throws Exception {
    AzureMonitorQueryClient mockClient = mock(AzureMonitorQueryClient.class);
    when(mockClient.executeQuery("query")).thenReturn(rows(5));
    AzureMonitorSourceFetcher fetcher = new AzureMonitorSourceFetcher("query", 2, mockClient);

    List<Map<String, String>> batch1 = fetcher.fetch();
    assertEquals(2, batch1.size());
    assertEquals("0", batch1.get(0).get("idx"));
    assertFalse(fetcher.isExhausted());

    List<Map<String, String>> batch2 = fetcher.fetch();
    assertEquals(2, batch2.size());
    assertEquals("2", batch2.get(0).get("idx"));
    assertFalse(fetcher.isExhausted());

    List<Map<String, String>> batch3 = fetcher.fetch();
    assertEquals(1, batch3.size());
    assertEquals("4", batch3.get(0).get("idx"));
    assertTrue(fetcher.isExhausted());

    verify(mockClient, times(1)).executeQuery("query");
  }

  @Test
  void emptyResult_isExhaustedImmediately_fetchReturnsEmpty() throws Exception {
    AzureMonitorQueryClient mockClient = mock(AzureMonitorQueryClient.class);
    when(mockClient.executeQuery("query")).thenReturn(List.of());
    AzureMonitorSourceFetcher fetcher = new AzureMonitorSourceFetcher("query", 10, mockClient);

    List<Map<String, String>> batch = fetcher.fetch();
    assertTrue(batch.isEmpty());
    assertTrue(fetcher.isExhausted());
  }

  @Test
  void fetchAfterExhaustion_returnsEmptyList() throws Exception {
    AzureMonitorQueryClient mockClient = mock(AzureMonitorQueryClient.class);
    when(mockClient.executeQuery("query")).thenReturn(rows(2));
    AzureMonitorSourceFetcher fetcher = new AzureMonitorSourceFetcher("query", 10, mockClient);

    fetcher.fetch(); // exhausts
    List<Map<String, String>> batch = fetcher.fetch(); // after exhaustion

    assertTrue(batch.isEmpty());
    assertTrue(fetcher.isExhausted());
    verify(mockClient, times(1)).executeQuery("query");
  }
}
