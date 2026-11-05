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

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigParser;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.BatchCommitStrategy;
import io.fleak.zephflow.lib.commands.source.MessageDecodingConverter;
import io.fleak.zephflow.lib.commands.source.SimpleSourceCommand;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class KafkaSourceCommitTest {

  private static final TopicPartition PARTITION = new TopicPartition("topic", 0);
  private static final Map<String, Object> CONFIG = Map.of("k", "v");

  private static class ProcessKilled extends Error {}

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
                        ("{\"i\":" + i + "}").getBytes()))
            .toList();
    when(consumer.poll(Duration.ofMillis(1000)))
        .thenReturn(new ConsumerRecords<>(Map.of(PARTITION, polled)));
    KafkaSourceFetcher fetcher =
        new KafkaSourceFetcher(consumer, null, BatchCommitStrategy.ofBatchSize(100));
    SourceExecutionContext<SerializedEvent> context =
        new SourceExecutionContext<>(
            fetcher,
            new MessageDecodingConverter(
                MessageDecodingConverter.decodingConfig(EncodingType.JSON_OBJECT, null, false),
                MessageDecodingConverter.Unit.MESSAGE),
            mock(),
            mock(),
            mock(),
            mock(),
            null);
    ConfigParser parser = mock();
    when(parser.parseConfig(CONFIG)).thenReturn(mock(CommandConfig.class));
    TestKafkaSource source = new TestKafkaSource(parser, context);
    source.parseAndValidateArg(CONFIG);
    source.initialize(mock(MetricClientProvider.class));

    assertThrows(ProcessKilled.class, () -> source.execute("user", killedAtRecord(201)));

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

  private static SourceEventAcceptor killedAtRecord(int record) {
    return new SourceEventAcceptor() {
      private int accepted;

      @Override
      public void accept(List<RecordFleakData> records) {
        if (++accepted == record) {
          throw new ProcessKilled();
        }
      }

      @Override
      public void terminate() {}
    };
  }

  private static class TestKafkaSource extends SimpleSourceCommand<SerializedEvent> {
    private final SourceExecutionContext<SerializedEvent> context;

    TestKafkaSource(ConfigParser parser, SourceExecutionContext<SerializedEvent> context) {
      super("source", mock(JobContext.class), parser, mock(ConfigValidator.class));
      this.context = context;
    }

    @Override
    public SourceType sourceType() {
      return SourceType.STREAMING;
    }

    @Override
    public String commandName() {
      return "kafkasource";
    }

    @Override
    protected SourceExecutionContext<SerializedEvent> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      return context;
    }
  }
}
