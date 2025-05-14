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

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

@Slf4j
public class KinesisSourceFetcher implements Fetcher<SerializedEvent> {

  private final ArrayBlockingQueue<KinesisClientRecord> recordQueue = new ArrayBlockingQueue<>(100);
  private final AtomicReference<RecordProcessorCheckpointer> lastSeenCheckpointer =
      new AtomicReference<>();
  private final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

  public KinesisSourceFetcher(KinesisSourceDto.Config config) {
    KinesisAsyncClient kinesisClient = KinesisAsyncClient.create();
    Region region = Region.of(config.getRegionStr());
    DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
    CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();
    ConfigsBuilder configsBuilder =
        new ConfigsBuilder(
            config.getStreamName(),
            config.getApplicationName(),
            kinesisClient,
            dynamoClient,
            cloudWatchClient,
            UUID.randomUUID().toString(),
            new RecordProcessorFactory(lastSeenCheckpointer, recordQueue));

    Scheduler scheduler =
        new Scheduler(
            configsBuilder.checkpointConfig(),
            configsBuilder.coordinatorConfig(),
            configsBuilder.leaseManagementConfig(),
            configsBuilder.lifecycleConfig(),
            configsBuilder.metricsConfig(),
            configsBuilder.processorConfig(),
            configsBuilder.retrievalConfig());

    EXECUTOR.submit(scheduler);
  }

  @SneakyThrows
  @Override
  public List<SerializedEvent> fetch() {
    log.trace("KinesisSource: fetch()");
    List<KinesisClientRecord> recordEvents = new ArrayList<>();
    recordQueue.drainTo(recordEvents, 500);

    List<SerializedEvent> events = new ArrayList<>(recordEvents.size());
    log.trace("Got record: {}", recordEvents.size());

    for (KinesisClientRecord r : recordEvents) {
      Map<String, String> metadata = new HashMap<>();
      metadata.put(METADATA_KINESIS_PARTITION_KEY, r.partitionKey());
      metadata.put(METADATA_KINESIS_SEQUENCE_NUMBER, r.sequenceNumber());
      metadata.put(METADATA_KINESIS_HASH_KEY, r.explicitHashKey());
      metadata.put(METADATA_KINESIS_SCHEMA_DATA_FORMAT, r.schema().getDataFormat());
      metadata.put(METADATA_KINESIS_SCHEMA_DEFINITION, r.schema().getSchemaDefinition());
      metadata.put(METADATA_KINESIS_SCHEMA_NAME, r.schema().getSchemaName());

      ByteBuffer dataBuff = r.data().duplicate();
      byte[] value = new byte[dataBuff.remaining()];
      dataBuff.get(value);

      SerializedEvent serializedEvent =
          new SerializedEvent(
              r.explicitHashKey().getBytes(StandardCharsets.UTF_8), value, metadata);

      events.add(serializedEvent);
    }

    return events;
  }

  @Override
  public Committer commiter() {
    return () -> {
      RecordProcessorCheckpointer checkpointer = lastSeenCheckpointer.get();
      if (checkpointer != null) {
        try {
          checkpointer.checkpoint();
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
      }
    };
  }

  @SneakyThrows
  @Override
  public void close() throws IOException {
    EXECUTOR.shutdown();
    if (!EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
      EXECUTOR.shutdownNow();
    }
  }

  private static class RecordProcessorFactory implements ShardRecordProcessorFactory {

    private final Queue<KinesisClientRecord> records;
    AtomicReference<RecordProcessorCheckpointer> lastSeenCheckpointer;

    private RecordProcessorFactory(
        AtomicReference<RecordProcessorCheckpointer> lastSeenCheckpointer,
        Queue<KinesisClientRecord> records) {
      this.lastSeenCheckpointer = lastSeenCheckpointer;
      this.records = records;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
      return new ZephflowShardRecordProcessor(lastSeenCheckpointer, records);
    }
  }

  private static class ZephflowShardRecordProcessor implements ShardRecordProcessor {

    private final Queue<KinesisClientRecord> records;
    AtomicReference<RecordProcessorCheckpointer> lastSeenCheckpointer;

    private ZephflowShardRecordProcessor(
        AtomicReference<RecordProcessorCheckpointer> lastSeenCheckpointer,
        Queue<KinesisClientRecord> records) {
      this.lastSeenCheckpointer = lastSeenCheckpointer;
      this.records = records;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
      log.info("Initializing KinesisSourceFetcher: " + initializationInput.toString());
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
      // Kinesis consumer uses a push model to get records, while the Zephflow source commands are
      // pull based.
      // To bridge this we push data to a blocking queue which Zephflow will pull from as it
      // processes the records.
      lastSeenCheckpointer.set(processRecordsInput.checkpointer());
      records.addAll(processRecordsInput.records());
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
      log.info("Lease lost: {}", leaseLostInput);
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
      log.info("Shard ended: {}", shardEndedInput);
      try {
        shardEndedInput.checkpointer().checkpoint();
      } catch (Exception e) {
        log.error("Error checkpointing at shard end: {}", e, e);
      }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
      log.info("Shutdown requested: {}", shutdownRequestedInput);
      try {
        shutdownRequestedInput.checkpointer().checkpoint();
      } catch (Exception e) {
        log.error("Error checkpointing at shutdown requested: {}", e, e);
      }
    }
  }
}
