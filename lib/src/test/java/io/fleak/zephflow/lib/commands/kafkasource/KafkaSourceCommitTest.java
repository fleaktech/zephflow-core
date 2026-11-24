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
package io.fleak.zephflow.lib.commands.kafkasource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.commands.source.BatchCommitStrategy;
import io.fleak.zephflow.lib.commands.source.SourceLoopHarness;
import io.fleak.zephflow.lib.commands.source.SourceLoopHarness.ProcessKilled;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

class KafkaSourceCommitTest {

  private static final TopicPartition PARTITION = new TopicPartition("topic", 0);

  @Test
  void aProcessKilledMidPollLeavesTheUnprocessedRecordsUncommitted() {
    KafkaConsumer<byte[], byte[]> consumer = mock();
    List<ConsumerRecord<byte[], byte[]>> polled =
        IntStream.range(0, 500)
            .mapToObj(
                i ->
                    new ConsumerRecord<>(
                        PARTITION.topic(),
                        PARTITION.partition(),
                        i,
                        "k".getBytes(),
                        "v".getBytes()))
            .toList();
    when(consumer.poll(Duration.ofMillis(1000)))
        .thenReturn(new ConsumerRecords<>(Map.of(PARTITION, polled)));
    KafkaSourceFetcher fetcher =
        new KafkaSourceFetcher(consumer, null, BatchCommitStrategy.ofBatchSize(100));

    assertThrows(
        ProcessKilled.class,
        () ->
            SourceLoopHarness.run(
                fetcher,
                SourceLoopHarness.oneEventPerRecord(),
                SourceLoopHarness.killedAt(201),
                null));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> committed =
        ArgumentCaptor.forClass(Map.class);
    verify(consumer, atLeast(0)).commitAsync(committed.capture(), any());
    assertEquals(
        List.of(
            Map.of(PARTITION, new OffsetAndMetadata(100)),
            Map.of(PARTITION, new OffsetAndMetadata(200))),
        committed.getAllValues());
    verify(consumer, never()).commitAsync(any(OffsetCommitCallback.class));
  }

  @Test
  void pauseCommitsTheLastProcessedOffsetSynchronouslyAndLeavesTheGroup() throws Exception {
    KafkaConsumer<byte[], byte[]> consumer = mock();
    List<ConsumerRecord<byte[], byte[]>> polled =
        IntStream.range(0, 5)
            .mapToObj(
                i ->
                    new ConsumerRecord<>(
                        PARTITION.topic(),
                        PARTITION.partition(),
                        i,
                        "k".getBytes(),
                        "v".getBytes()))
            .toList();
    when(consumer.poll(Duration.ofMillis(1000)))
        .thenReturn(new ConsumerRecords<>(Map.of(PARTITION, polled)));
    when(consumer.subscription()).thenReturn(Set.of(PARTITION.topic()));
    when(consumer.assignment()).thenReturn(Set.of(PARTITION));
    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(consumer, null);

    fetcher.fetch();
    fetcher.committer().commitThrough(5);
    fetcher.pause();

    InOrder order = inOrder(consumer);
    order.verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(5)));
    order.verify(consumer).unsubscribe();

    fetcher.resume();
    verify(consumer).subscribe(eq(Set.of(PARTITION.topic())), any(ConsumerRebalanceListener.class));
  }

  @Test
  void pauseCommitsOnlyThePartitionsStillAssigned() throws Exception {
    KafkaConsumer<byte[], byte[]> consumer = mock();
    TopicPartition revoked = new TopicPartition("topic", 1);
    when(consumer.poll(Duration.ofMillis(1000)))
        .thenReturn(
            new ConsumerRecords<>(
                Map.of(
                    PARTITION,
                    List.of(record(PARTITION, 7)),
                    revoked,
                    List.of(record(revoked, 100)))));
    when(consumer.assignment()).thenReturn(Set.of(PARTITION));
    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(consumer, null);

    fetcher.fetch();
    fetcher.committer().commitThrough(2);
    fetcher.pause();

    verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(8)));
  }

  private static ConsumerRecord<byte[], byte[]> record(TopicPartition partition, long offset) {
    return new ConsumerRecord<>(
        partition.topic(), partition.partition(), offset, "k".getBytes(), "v".getBytes());
  }
}
