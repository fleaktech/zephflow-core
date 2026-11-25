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
package io.fleak.zephflow.lib.commands.kinesissource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.lib.commands.source.SourceBacklog;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;

class KinesisSourceFetcherBacklogTest {

  @Test
  void reportsTheLatestMillisBehindOfTheFurthestBehindShardAsATime() {
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher();
    ShardRecordProcessor shard1 = shard(fetcher, "shard-1");
    ShardRecordProcessor shard2 = shard(fetcher, "shard-2");

    batch(shard1, 9_000L);
    batch(shard1, 3_000L);
    batch(shard2, 7_000L);

    assertEquals(SourceBacklog.millisBehind(7_000L), fetcher.backlog());
    assertEquals(SourceBacklog.Unit.MILLIS, fetcher.backlog().unit());
  }

  @Test
  void aShardWhoseLeaseIsLostNoLongerCounts() {
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher();
    ShardRecordProcessor shard1 = shard(fetcher, "shard-1");
    ShardRecordProcessor shard2 = shard(fetcher, "shard-2");
    batch(shard1, 3_000L);
    batch(shard2, 7_000L);

    shard2.leaseLost(LeaseLostInput.builder().build());

    assertEquals(SourceBacklog.millisBehind(3_000L), fetcher.backlog());
  }

  @Test
  void noShardReportedYetIsUnknown() {
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher();
    shard(fetcher, "shard-1");

    assertEquals(SourceBacklog.UNKNOWN, fetcher.backlog());
  }

  private static ShardRecordProcessor shard(KinesisSourceFetcher fetcher, String shardId) {
    ShardRecordProcessor processor = fetcher.newShardRecordProcessor();
    processor.initialize(InitializationInput.builder().shardId(shardId).build());
    return processor;
  }

  private static void batch(ShardRecordProcessor processor, long millisBehindLatest) {
    processor.processRecords(
        ProcessRecordsInput.builder()
            .records(List.of())
            .checkpointer(mock(RecordProcessorCheckpointer.class))
            .millisBehindLatest(millisBehindLatest)
            .build());
  }
}
