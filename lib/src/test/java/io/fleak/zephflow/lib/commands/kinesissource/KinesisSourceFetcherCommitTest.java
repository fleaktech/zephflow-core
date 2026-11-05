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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.commands.source.Fetcher;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

class KinesisSourceFetcherCommitTest {

  private final List<String> checkpoints = new ArrayList<>();

  @Test
  void commitThroughCheckpointsEachShardAtItsLastHandledRecord() throws Exception {
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher();
    RecordProcessorCheckpointer shard1 = checkpointer("shard-1");
    RecordProcessorCheckpointer shard2 = checkpointer("shard-2");
    deliver(fetcher, "shard-1", shard1, record("1"), record("2"), noData("3"), record("4"));
    deliver(fetcher, "shard-2", shard2, record("10"));
    Fetcher.Committer committer = fetcher.committer();
    List<List<String>> perCommit = new ArrayList<>();

    assertEquals(4, fetcher.fetch().size());
    committer.commitThrough(1);
    perCommit.add(drainCheckpoints());
    committer.commitThrough(3);
    perCommit.add(drainCheckpoints());
    committer.commitThrough(4);
    perCommit.add(drainCheckpoints());

    assertEquals(
        List.of(List.of("shard-1@1"), List.of("shard-1@4"), List.of("shard-2@10")), perCommit);
  }

  @Test
  void aShutdownRequestDoesNotCheckpointQueuedRecords() throws Exception {
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher();
    RecordProcessorCheckpointer shard1 = checkpointer("shard-1");
    ShardRecordProcessor processor = deliver(fetcher, "shard-1", shard1, record("1"));

    processor.shutdownRequested(ShutdownRequestedInput.builder().checkpointer(shard1).build());

    verifyNoInteractions(shard1);
  }

  private static ShardRecordProcessor deliver(
      KinesisSourceFetcher fetcher,
      String shardId,
      RecordProcessorCheckpointer checkpointer,
      KinesisClientRecord... records) {
    ShardRecordProcessor processor = fetcher.newShardRecordProcessor();
    processor.initialize(InitializationInput.builder().shardId(shardId).build());
    processor.processRecords(
        ProcessRecordsInput.builder().records(List.of(records)).checkpointer(checkpointer).build());
    return processor;
  }

  private RecordProcessorCheckpointer checkpointer(String shardId) throws Exception {
    RecordProcessorCheckpointer checkpointer = mock();
    doAnswer(call -> checkpoints.add(shardId + "@" + call.<String>getArgument(0)))
        .when(checkpointer)
        .checkpoint(anyString(), anyLong());
    return checkpointer;
  }

  private List<String> drainCheckpoints() {
    List<String> drained = List.copyOf(checkpoints);
    checkpoints.clear();
    return drained;
  }

  private static KinesisClientRecord record(String sequenceNumber) {
    return KinesisClientRecord.builder()
        .sequenceNumber(sequenceNumber)
        .subSequenceNumber(0)
        .partitionKey("pk")
        .data(ByteBuffer.wrap("{}".getBytes(UTF_8)))
        .build();
  }

  private static KinesisClientRecord noData(String sequenceNumber) {
    return KinesisClientRecord.builder()
        .sequenceNumber(sequenceNumber)
        .subSequenceNumber(0)
        .partitionKey("pk")
        .build();
  }
}
