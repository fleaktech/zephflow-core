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

import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.*;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

@Slf4j
public class KinesisSourceFetcher implements Fetcher<SerializedEvent> {

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final ArrayBlockingQueue<KinesisClientRecord> recordQueue = new ArrayBlockingQueue<>(500);
  private final AtomicReference<RecordProcessorCheckpointer> lastSeenCheckpointer =
      new AtomicReference<>();
  private final AtomicReference<Scheduler> schedulerRef = new AtomicReference<>();

  private final ExecutorService EXECUTOR =
      Executors.newSingleThreadExecutor(
          runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("KinesisSourceFetcher");
            thread.setDaemon(true);
            return thread;
          });

  @SneakyThrows
  public KinesisSourceFetcher(@NonNull KinesisSourceDto.Config config) {
    AwsCredentialsProvider credentialsProvider = null;

    if (config.getStaticCredentials() != null) {
      credentialsProvider =
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(
                  config.getStaticCredentials().key(), config.getStaticCredentials().secret()));
    }

    KinesisAsyncClient kinesisClient =
        AwsClientFactory.getKinesisAsyncClient(config.getKinesisEndpoint(), credentialsProvider);

    Region region = Region.of(config.getRegionStr());
    DynamoDbAsyncClient dynamoClient =
        AwsClientFactory.getDynamoDbAsyncClient(
            region, config.getDynamoEndpoint(), credentialsProvider);

    CloudWatchAsyncClient cloudWatchClient =
        AwsClientFactory.getCloudWatchAsyncClient(
            region, config.getCloudWatchEndpoint(), credentialsProvider);

    var recordProcessorFactory = new RecordProcessorFactory(lastSeenCheckpointer, recordQueue);
    ConfigsBuilder configsBuilder =
        new ConfigsBuilder(
            config.getStreamName(),
            config.getApplicationName(),
            kinesisClient,
            dynamoClient,
            cloudWatchClient,
            UUID.randomUUID().toString(),
            recordProcessorFactory);

    if (config.isDisableMetrics()) {
      configsBuilder.metricsConfig().metricsFactory(new NullMetricsFactory());
    }

    // set the start position the first time we start reading from a stream
    var streamTracker = configureStreamTracker(config);
    var retrievalConfig = configsBuilder.retrievalConfig().streamTracker(streamTracker);

    Scheduler scheduler =
        new Scheduler(
            configsBuilder.checkpointConfig(),
            configsBuilder.coordinatorConfig(),
            configsBuilder.leaseManagementConfig(),
            configsBuilder.lifecycleConfig(),
            configsBuilder.metricsConfig(),
            new ProcessorConfig(recordProcessorFactory),
            retrievalConfig);
    schedulerRef.set(scheduler);
  }

  public void start() {
    if (!started.compareAndSet(false, true)) {
      log.warn("scheduler was already started");
      return;
    }
    var scheduler = schedulerRef.get();
    EXECUTOR.submit(
        () -> {
          try {
            // ensure we print an error message if the scheduler fails to initialise
            scheduler.run();
          } catch (Exception e) {
            log.error(e.getMessage(), e);
          }
        });
  }

  private SingleStreamTracker configureStreamTracker(KinesisSourceDto.Config config) {
    InitialPositionInStreamExtended initial =
        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

    if (config.getInitialPositionTimestamp() != null) {
      initial =
          InitialPositionInStreamExtended.newInitialPositionAtTimestamp(
              config.getInitialPositionTimestamp());
    } else if (config.getInitialPosition() != null) {
      initial = InitialPositionInStreamExtended.newInitialPosition(config.getInitialPosition());
    }

    return new SingleStreamTracker(config.getStreamName(), initial);
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
      if (r.schema() != null) {
        metadata.put(METADATA_KINESIS_SCHEMA_DATA_FORMAT, r.schema().getDataFormat());
        metadata.put(METADATA_KINESIS_SCHEMA_DEFINITION, r.schema().getSchemaDefinition());
        metadata.put(METADATA_KINESIS_SCHEMA_NAME, r.schema().getSchemaName());
      }

      byte[] value = SdkBytes.fromByteBuffer(r.data()).asByteArray();

      byte[] key = null;
      if (r.explicitHashKey() != null) {
        key = r.explicitHashKey().getBytes(StandardCharsets.UTF_8);
      }

      SerializedEvent serializedEvent = new SerializedEvent(key, value, metadata);

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
          log.error("Commiter failed: ", e);
        }
      }
    };
  }

  @SneakyThrows
  @Override
  public void close() throws IOException {
    var scheduler = schedulerRef.get();
    if (scheduler != null) {
      scheduler.shutdown();
    }

    EXECUTOR.shutdown();
    if (!EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
      EXECUTOR.shutdownNow();
    }
    started.set(false);
  }

  @Slf4j
  public static class RecordProcessorFactory implements ShardRecordProcessorFactory {

    private final BlockingQueue<KinesisClientRecord> records;
    AtomicReference<RecordProcessorCheckpointer> lastSeenCheckpointer;

    private RecordProcessorFactory(
        AtomicReference<RecordProcessorCheckpointer> lastSeenCheckpointer,
        BlockingQueue<KinesisClientRecord> records) {
      this.lastSeenCheckpointer = lastSeenCheckpointer;
      this.records = records;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor(StreamIdentifier streamIdentifier) {
      return shardRecordProcessor();
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
      return new ZephflowShardRecordProcessor(lastSeenCheckpointer, records);
    }
  }

  @Slf4j
  public static class ZephflowShardRecordProcessor implements ShardRecordProcessor {

    private final BlockingQueue<KinesisClientRecord> records;
    AtomicReference<RecordProcessorCheckpointer> lastSeenCheckpointer;

    private ZephflowShardRecordProcessor(
        AtomicReference<RecordProcessorCheckpointer> lastSeenCheckpointer,
        BlockingQueue<KinesisClientRecord> records) {
      log.info("Starting ZephflowShardRecordProcessor processor");
      this.lastSeenCheckpointer = lastSeenCheckpointer;
      this.records = records;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
      log.info("Initializing KinesisSourceFetcher: {}", initializationInput.toString());
    }

    @SneakyThrows
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
      // Kinesis consumer uses a push model to get records, while the Zephflow source commands are
      // pull based.
      // To bridge this we push data to a blocking queue which Zephflow will pull from as it
      // processes the records.
      lastSeenCheckpointer.set(processRecordsInput.checkpointer());
      for (var record : processRecordsInput.records()) {
        try {
          records.put(record);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Interrupted while adding to queue", e);
          return;
        }
      }
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
