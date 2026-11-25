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

import io.fleak.zephflow.lib.commands.source.BatchCommitStrategy;
import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.SourceBacklog;
import io.fleak.zephflow.lib.kafka.KafkaHealthMonitor;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/** Created by bolei on 9/24/24 */
@Slf4j
public final class KafkaSourceFetcher implements Fetcher<SerializedEvent> {

  private static final LogOffsetCommitCallback COMMIT_CALLBACK = new LogOffsetCommitCallback();

  private final KafkaConsumer<byte[], byte[]> consumer;
  private final KafkaHealthMonitor healthMonitor;
  private final CommitStrategy commitStrategy;
  private final Function<ConsumerRecord<byte[], byte[]>, Map<String, String>> metadataExtractor;

  // Partition and offset of each record of the latest fetch, in the order fetch() returned them.
  private List<TopicPartition> fetchedPartitions = List.of();
  private long[] fetchedOffsets = new long[0];

  // Offsets handed to commitAsync since the last pause, re-committed synchronously on pause.
  private final Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
  private Set<String> pausedSubscription = Set.of();

  // Computed on the polling thread because KafkaConsumer may not be used from other threads.
  private volatile SourceBacklog backlog = SourceBacklog.UNKNOWN;

  public KafkaSourceFetcher(
      KafkaConsumer<byte[], byte[]> consumer,
      KafkaHealthMonitor healthMonitor,
      CommitStrategy commitStrategy,
      Function<ConsumerRecord<byte[], byte[]>, Map<String, String>> metadataExtractor) {
    this.consumer = consumer;
    this.healthMonitor = healthMonitor;
    this.commitStrategy = commitStrategy;
    this.metadataExtractor = metadataExtractor;
  }

  public KafkaSourceFetcher(
      KafkaConsumer<byte[], byte[]> consumer,
      KafkaHealthMonitor healthMonitor,
      CommitStrategy commitStrategy) {
    this(consumer, healthMonitor, commitStrategy, record -> null);
  }

  public KafkaSourceFetcher(
      KafkaConsumer<byte[], byte[]> consumer, KafkaHealthMonitor healthMonitor) {
    this(consumer, healthMonitor, BatchCommitStrategy.forKafka());
  }

  @Override
  public List<SerializedEvent> fetch() {
    log.trace("KafkaSourceFetcher: fetch()");
    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));

    log.debug("Got records: {}", records.count());

    List<SerializedEvent> rawEvents = new ArrayList<>();
    List<TopicPartition> partitions = new ArrayList<>(records.count());
    long[] offsets = new long[records.count()];

    for (ConsumerRecord<byte[], byte[]> r : records) {
      SerializedEvent serializedEvent =
          new SerializedEvent(r.key(), r.value(), metadataExtractor.apply(r));
      offsets[rawEvents.size()] = r.offset();
      partitions.add(new TopicPartition(r.topic(), r.partition()));
      rawEvents.add(serializedEvent);
    }
    fetchedPartitions = partitions;
    fetchedOffsets = offsets;
    try {
      backlog = SourceBacklog.records(recordsLag(), partitionCount());
    } catch (RuntimeException e) {
      log.debug("could not read the consumer backlog", e);
      backlog = SourceBacklog.UNKNOWN;
    }
    return rawEvents;
  }

  @Override
  public SourceBacklog backlog() {
    return backlog;
  }

  /** Null unless every assigned partition has a lag sample, so a rebalance never reads as zero. */
  private Long recordsLag() {
    Set<TopicPartition> assigned = consumer.assignment();
    if (assigned.isEmpty()) {
      return null;
    }
    Map<TopicPartition, Double> lagByPartition = new HashMap<>();
    for (Map.Entry<MetricName, ? extends Metric> entry : consumer.metrics().entrySet()) {
      MetricName name = entry.getKey();
      if (!"records-lag".equals(name.name())
          || !"consumer-fetch-manager-metrics".equals(name.group())) {
        continue;
      }
      String topic = name.tags().get("topic");
      String partition = name.tags().get("partition");
      if (topic == null
          || partition == null
          || !(entry.getValue().metricValue() instanceof Double lag)) {
        continue;
      }
      lagByPartition.put(new TopicPartition(topic, Integer.parseInt(partition)), lag);
    }
    long total = 0;
    for (TopicPartition tp : assigned) {
      Double lag = lagByPartition.get(tp);
      if (lag == null || lag.isNaN()) {
        return null;
      }
      total += lag.longValue();
    }
    return total;
  }

  private Integer partitionCount() {
    Set<String> topics = consumer.subscription();
    if (topics.isEmpty()) {
      return null;
    }
    int count = 0;
    for (String topic : topics) {
      List<PartitionInfo> partitions = consumer.partitionsFor(topic);
      if (partitions == null || partitions.isEmpty()) {
        return null;
      }
      count += partitions.size();
    }
    return count;
  }

  @Override
  public CommitStrategy commitStrategy() {
    return commitStrategy;
  }

  @Override
  public Committer committer() {
    return new Committer() {
      @Override
      public void commit() {
        consumer.commitAsync(COMMIT_CALLBACK);
      }

      @Override
      public void commitThrough(int handledRecords) {
        Map<TopicPartition, OffsetAndMetadata> next = new HashMap<>();
        for (int i = 0; i < handledRecords; i++) {
          next.put(fetchedPartitions.get(i), new OffsetAndMetadata(fetchedOffsets[i] + 1));
        }
        if (!next.isEmpty()) {
          consumer.commitAsync(next, COMMIT_CALLBACK);
          committedOffsets.putAll(next);
        }
      }
    };
  }

  /** Commits synchronously, then leaves the group so its partitions move to other consumers. */
  @Override
  public void pause() {
    // A partition lost in a rebalance may already have a newer offset from its new owner.
    committedOffsets.keySet().retainAll(consumer.assignment());
    consumer.commitSync(Map.copyOf(committedOffsets));
    committedOffsets.clear();
    pausedSubscription = Set.copyOf(consumer.subscription());
    consumer.unsubscribe();
    fetchedPartitions = List.of();
    fetchedOffsets = new long[0];
    backlog = SourceBacklog.UNKNOWN;
  }

  @Override
  public void resume() {
    subscribe(consumer, pausedSubscription);
  }

  static void subscribe(KafkaConsumer<byte[], byte[]> consumer, Collection<String> topics) {
    consumer.subscribe(
        topics,
        new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info(
                "Partitions revoked: {}",
                partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(",")));
          }

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info(
                "Partitions assigned: {}",
                partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(",")));
          }
        });
  }

  @Override
  public void close() throws IOException {
    if (consumer != null) {
      consumer.close();
    }
    if (healthMonitor != null) {
      healthMonitor.stop();
    }
  }

  private static class LogOffsetCommitCallback implements OffsetCommitCallback {
    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
      if (exception != null) log.error("error committing offsets", exception);
      else log.debug("{}", offsets);
    }
  }
}
