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
import io.fleak.zephflow.lib.kafka.KafkaHealthMonitor;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

/** Created by bolei on 9/24/24 */
@Slf4j
public record KafkaSourceFetcher(
    KafkaConsumer<byte[], byte[]> consumer,
    KafkaHealthMonitor healthMonitor,
    CommitStrategy commitStrategy)
    implements Fetcher<SerializedEvent> {

  private static final LogOffsetCommitCallback COMMIT_CALLBACK = new LogOffsetCommitCallback();

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

    for (ConsumerRecord<byte[], byte[]> r : records) {
      SerializedEvent serializedEvent = new SerializedEvent(r.key(), r.value(), null);
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
