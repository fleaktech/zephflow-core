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

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.kafka.KafkaHealthMonitor;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

/** Created by bolei on 9/24/24 */
@Slf4j
public class KafkaSourceFetcher implements Fetcher<SerializedEvent> {

  private static final LogOffsetCommitCallback COMMIT_CALLBACK = new LogOffsetCommitCallback();

  final KafkaConsumer<byte[], byte[]> consumer;
  private final KafkaHealthMonitor healthMonitor;

  public KafkaSourceFetcher(
      KafkaConsumer<byte[], byte[]> consumer, KafkaHealthMonitor healthMonitor) {
    this.consumer = consumer;
    this.healthMonitor = healthMonitor;
  }

  @Override
  public List<SerializedEvent> fetch() {
    log.debug("KafkaSourceFetcher: fetch()");
    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));

    log.debug("Got records: {}", records.count());

    List<SerializedEvent> rawEvents = new ArrayList<>();

    for (ConsumerRecord<byte[], byte[]> r : records) {
      Map<String, String> metadata = new HashMap<>();
      metadata.put(METADATA_KAFKA_TOPIC, r.topic());
      metadata.put(METADATA_KAFKA_PARTITION, Integer.toString(r.partition()));
      metadata.put(METADATA_KAFKA_OFFSET, Long.toString(r.offset()));
      metadata.put(METADATA_KAFKA_TIMESTAMP, Long.toString(r.timestamp()));
      metadata.put(METADATA_KAFKA_TIMESTAMP_TYPE, r.timestampType().toString());
      metadata.put(METADATA_KAFKA_SERIALIZED_KEY_SIZE, Integer.toString(r.serializedKeySize()));
      metadata.put(METADATA_KAFKA_SERIALIZED_VALUE_SIZE, Integer.toString(r.serializedValueSize()));
      metadata.put(
          METADATA_KAFKA_LEADER_EPOCH,
          r.leaderEpoch().map(epoc -> Integer.toString(epoc)).orElse(null));
      r.headers()
          .forEach(
              h -> metadata.put(METADATA_KAFKA_HEADER_PREFIX + h.key(), toBase64String(h.value())));
      SerializedEvent serializedEvent = new SerializedEvent(r.key(), r.value(), metadata);
      rawEvents.add(serializedEvent);
    }
    return rawEvents;
  }

  @Override
  public Committer commiter() {
    return () -> consumer.commitAsync(COMMIT_CALLBACK);
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
