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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.commands.source.SourceBacklog;
import java.util.*;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaSourceFetcherBacklogTest {

  private static final String TOPIC = "orders.v1";

  private KafkaConsumer<byte[], byte[]> consumer;
  private final Map<MetricName, Metric> metrics = new HashMap<>();

  @BeforeEach
  void setUp() {
    consumer = mock();
    when(consumer.poll(any())).thenReturn(new ConsumerRecords<>(Map.of()));
    when(consumer.subscription()).thenReturn(Set.of(TOPIC));
    when(consumer.partitionsFor(TOPIC))
        .thenReturn(
            IntStream.range(0, 6)
                .mapToObj(p -> new PartitionInfo(TOPIC, p, null, null, null))
                .toList());
    doReturn(metrics).when(consumer).metrics();
  }

  @Test
  void sumsTheLagOfAssignedPartitionsAndReportsTheTopicPartitionCount() {
    assign(0, 1);
    lag(0, 100);
    lag(1, 50);
    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(consumer, null);

    fetcher.fetch();

    assertEquals(SourceBacklog.records(150L, 6), fetcher.backlog());
  }

  @Test
  void anAssignedPartitionWithoutALagMetricMakesTheBacklogUnknown() {
    assign(0, 1);
    lag(0, 100);
    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(consumer, null);

    fetcher.fetch();

    assertEquals(SourceBacklog.records(null, 6), fetcher.backlog());
  }

  @Test
  void aLagMetricWithNoSampleYetMakesTheBacklogUnknown() {
    assign(0);
    metrics.put(lagName(0), metric(Double.NaN));
    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(consumer, null);

    fetcher.fetch();

    assertEquals(SourceBacklog.records(null, 6), fetcher.backlog());
  }

  @Test
  void noAssignedPartitionsIsUnknownRatherThanZero() {
    assign();
    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(consumer, null);

    fetcher.fetch();

    assertEquals(SourceBacklog.records(null, 6), fetcher.backlog());
  }

  @Test
  void unrelatedAndMalformedMetricsAreIgnored() {
    assign(0);
    lag(0, 100);
    metrics.put(
        new MetricName("records-lag", "other-group", "", Map.of("topic", TOPIC, "partition", "0")),
        metric(5));
    metrics.put(
        new MetricName("records-lag", "consumer-fetch-manager-metrics", "", Map.of("topic", TOPIC)),
        metric(5));
    metrics.put(
        new MetricName(
            "records-lag", "consumer-fetch-manager-metrics", "", Map.of("partition", "0")),
        metric(5));
    Metric notANumber = mock();
    when(notANumber.metricValue()).thenReturn("n/a");
    metrics.put(
        new MetricName(
            "records-lag",
            "consumer-fetch-manager-metrics",
            "",
            Map.of("client-id", "c2", "topic", TOPIC, "partition", "0")),
        notANumber);
    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(consumer, null);

    fetcher.fetch();

    assertEquals(SourceBacklog.records(100L, 6), fetcher.backlog());
  }

  @Test
  void aTopicWithoutPartitionMetadataMakesThePartitionCountUnknown() {
    assign(0);
    lag(0, 100);
    when(consumer.partitionsFor(TOPIC)).thenReturn(null);
    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(consumer, null);

    fetcher.fetch();

    assertEquals(SourceBacklog.records(100L, null), fetcher.backlog());
  }

  @Test
  void aFailedBacklogReadIsUnknown() {
    assign(0);
    when(consumer.metrics()).thenThrow(new IllegalStateException("closed"));
    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(consumer, null);

    fetcher.fetch();

    assertEquals(SourceBacklog.UNKNOWN, fetcher.backlog());
  }

  @Test
  void beforeTheFirstFetchTheBacklogIsUnknown() {
    assertEquals(SourceBacklog.UNKNOWN, new KafkaSourceFetcher(consumer, null).backlog());
  }

  private void assign(int... partitions) {
    Set<TopicPartition> assigned = new HashSet<>();
    for (int p : partitions) {
      assigned.add(new TopicPartition(TOPIC, p));
    }
    when(consumer.assignment()).thenReturn(assigned);
  }

  private void lag(int partition, double lag) {
    metrics.put(lagName(partition), metric(lag));
    // The deprecated duplicate with dots replaced in the topic tag must not be counted twice.
    metrics.put(
        new MetricName(
            "records-lag",
            "consumer-fetch-manager-metrics",
            "",
            Map.of(
                "client-id", "c1",
                "topic", TOPIC.replace('.', '_'),
                "partition", String.valueOf(partition))),
        metric(lag));
  }

  private static MetricName lagName(int partition) {
    return new MetricName(
        "records-lag",
        "consumer-fetch-manager-metrics",
        "",
        Map.of("client-id", "c1", "topic", TOPIC, "partition", String.valueOf(partition)));
  }

  private static Metric metric(double value) {
    Metric metric = mock();
    when(metric.metricValue()).thenReturn(value);
    return metric;
  }
}
