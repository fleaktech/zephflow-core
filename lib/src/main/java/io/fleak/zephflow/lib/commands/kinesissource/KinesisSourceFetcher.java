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
import io.fleak.zephflow.lib.commands.source.BatchCommitStrategy;
import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.FetchProgress;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.SourceBacklog;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
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

  // Checkpoints handled records once a second at most, and at the end of each fetch.
  private static final CommitStrategy COMMIT_STRATEGY = BatchCommitStrategy.ofInterval(1000);
  private static final Duration GRACEFUL_SHUTDOWN_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration RUN_EXIT_TIMEOUT = Duration.ofSeconds(30);

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicReference<SchedulerRunState> runState =
      new AtomicReference<>(new SchedulerRunState(null, null));

  private final ArrayBlockingQueue<ShardRecord> recordQueue = new ArrayBlockingQueue<>(500);
  private final Map<String, Long> millisBehindByShard = new ConcurrentHashMap<>();
  private final AtomicReference<Scheduler> schedulerRef = new AtomicReference<>();
  private volatile Future<?> schedulerRun;
  private final Supplier<Scheduler> schedulerFactory;
  private final Duration runExitTimeout;
  // Set while paused: shard processors drop records instead of queueing them.
  private final AtomicBoolean releasing = new AtomicBoolean(false);
  private final Map<String, ShardRecord> lastCheckpointedByShard = new ConcurrentHashMap<>();

  private final FetchProgress<ShardRecord> progress = new FetchProgress<>();
  private final Map<String, ShardRecord> latestHandledByShard = new LinkedHashMap<>();
  // For each event of the latest fetch, the number of drained records up to and including its own.
  private int[] drainedThroughEvent = new int[0];

  private final ExecutorService EXECUTOR =
      Executors.newSingleThreadExecutor(
          runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("KinesisSourceFetcher");
            thread.setDaemon(true);
            return thread;
          });

  public KinesisSourceFetcher(
      @NonNull KinesisSourceDto.Config config, AwsCredentialsProvider credentialsProvider) {
    this.schedulerFactory = () -> newScheduler(config, credentialsProvider);
    this.runExitTimeout = RUN_EXIT_TIMEOUT;
    schedulerRef.set(schedulerFactory.get());
  }

  /** A fetcher whose scheduler comes from {@code schedulerFactory}, which may return null. */
  KinesisSourceFetcher(Supplier<Scheduler> schedulerFactory) {
    this(schedulerFactory, RUN_EXIT_TIMEOUT);
  }

  KinesisSourceFetcher(Supplier<Scheduler> schedulerFactory, Duration runExitTimeout) {
    this.schedulerFactory = schedulerFactory;
    this.runExitTimeout = runExitTimeout;
    schedulerRef.set(schedulerFactory.get());
  }

  /** A fetcher with no scheduler; records reach it through {@link #newShardRecordProcessor()}. */
  KinesisSourceFetcher() {
    this(() -> null);
  }

  @SneakyThrows
  private Scheduler newScheduler(
      KinesisSourceDto.Config config, AwsCredentialsProvider credentialsProvider) {
    KinesisAsyncClient kinesisClient =
        AwsClientFactory.getKinesisAsyncClient(
            Region.of(config.getRegionStr()), config.getKinesisEndpoint(), credentialsProvider);

    Region region = Region.of(config.getRegionStr());
    DynamoDbAsyncClient dynamoClient =
        AwsClientFactory.getDynamoDbAsyncClient(
            region, config.getDynamoEndpoint(), credentialsProvider);

    CloudWatchAsyncClient cloudWatchClient =
        AwsClientFactory.getCloudWatchAsyncClient(
            region, config.getCloudWatchEndpoint(), credentialsProvider);

    var recordProcessorFactory =
        new RecordProcessorFactory(
            recordQueue, millisBehindByShard, releasing, lastCheckpointedByShard);
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

    return new Scheduler(
        configsBuilder.checkpointConfig(),
        configsBuilder.coordinatorConfig(),
        configsBuilder.leaseManagementConfig(),
        configsBuilder.lifecycleConfig(),
        configsBuilder.metricsConfig(),
        new ProcessorConfig(recordProcessorFactory),
        retrievalConfig);
  }

  ShardRecordProcessor newShardRecordProcessor() {
    return new ZephflowShardRecordProcessor(
        recordQueue, millisBehindByShard, releasing, lastCheckpointedByShard);
  }

  @Override
  public SourceBacklog backlog() {
    return millisBehindByShard.values().stream()
        .max(Long::compare)
        .map(SourceBacklog::millisBehind)
        .orElse(SourceBacklog.UNKNOWN);
  }

  public void start() {
    if (!started.compareAndSet(false, true)) {
      log.warn("scheduler was already started");
      return;
    }
    var scheduler = Objects.requireNonNull(schedulerRef.get());
    var running = new SchedulerRunState(scheduler, null);
    runState.set(running);

    schedulerRun =
        EXECUTOR.submit(
            () -> {
              Exception failure = null;
              try {
                // ensure we print an error message if the scheduler fails to initialise
                scheduler.run();
              } catch (Exception e) {
                log.error(e.getMessage(), e);
                failure = e;
              } finally {
                // A scheduler replaced by resume() must not report its successor as stopped.
                runState.compareAndSet(running, new SchedulerRunState(null, failure));
              }
            });
  }

  private record SchedulerRunState(Scheduler scheduler, Exception error) {}

  private void awaitSchedulerRunExit() {
    Future<?> run = schedulerRun;
    if (run == null) {
      return;
    }
    try {
      run.get(runExitTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      log.warn("Kinesis scheduler still running after {}", runExitTimeout);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      log.warn("Kinesis scheduler run failed", e);
    }
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
    var drained = new ArrayList<ShardRecord>();
    recordQueue.drainTo(drained, 500);

    var events = new ArrayList<SerializedEvent>(drained.size());
    var drainedThrough = new int[drained.size()];
    log.debug("Got record: {}", drained.size());

    for (int i = 0; i < drained.size(); i++) {
      var r = drained.get(i).record();
      var serializedEvent = getSerializedEvent(r, getMetadata(r));
      if (serializedEvent == null) continue;
      drainedThrough[events.size()] = i + 1;
      events.add(serializedEvent);
    }
    drainedThroughEvent = Arrays.copyOf(drainedThrough, events.size());
    markHandled(progress.next(drained));

    if (events.isEmpty()) {
      checkSchedulerIsRunning();
    }

    return events;
  }

  private void checkSchedulerIsRunning() {
    // first process all messages
    // and then throw any exceptions or report not running
    SchedulerRunState state = runState.get();
    if (state.error() != null) {
      throw new KinesisSourceFetcherError("kinesis scheduler error", state.error());
    }
    if (state.scheduler() == null) {
      throw new KinesisSourceFetcherError("The kinesis scheduler stopped running");
    }
  }

  private static @NotNull Map<String, String> getMetadata(KinesisClientRecord r) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(METADATA_KINESIS_PARTITION_KEY, r.partitionKey());
    metadata.put(METADATA_KINESIS_SEQUENCE_NUMBER, r.sequenceNumber());
    metadata.put(METADATA_KINESIS_HASH_KEY, r.explicitHashKey());
    var schema = r.schema();
    if (schema != null) {
      metadata.put(METADATA_KINESIS_SCHEMA_DATA_FORMAT, schema.getDataFormat());
      metadata.put(METADATA_KINESIS_SCHEMA_DEFINITION, schema.getSchemaDefinition());
      metadata.put(METADATA_KINESIS_SCHEMA_NAME, schema.getSchemaName());
    }
    return metadata;
  }

  private static @Nullable SerializedEvent getSerializedEvent(
      KinesisClientRecord r, Map<String, String> metadata) {
    var data = r.data();
    if (data == null) {
      return null;
    }

    var value = SdkBytes.fromByteBuffer(data).asByteArray();

    byte[] key = null;
    var hashKey = r.explicitHashKey();
    if (hashKey != null) {
      key = hashKey.getBytes(StandardCharsets.UTF_8);
    }

    return new SerializedEvent(key, value, metadata);
  }

  @Override
  public CommitStrategy commitStrategy() {
    return COMMIT_STRATEGY;
  }

  @Override
  public Committer committer() {
    return new Committer() {
      @Override
      public void commit() {
        markHandled(progress.rest());
        checkpointHandled();
      }

      @Override
      public void commitThrough(int handledRecords) {
        int events = Math.min(handledRecords, drainedThroughEvent.length);
        markHandled(progress.through(events == 0 ? 0 : drainedThroughEvent[events - 1]));
        checkpointHandled();
      }
    };
  }

  private void markHandled(List<ShardRecord> handled) {
    for (ShardRecord shardRecord : handled) {
      latestHandledByShard.put(shardRecord.shardId(), shardRecord);
    }
  }

  private void checkpointHandled() {
    for (ShardRecord handled : latestHandledByShard.values()) {
      try {
        handled
            .checkpointer()
            .checkpoint(handled.record().sequenceNumber(), handled.record().subSequenceNumber());
        lastCheckpointedByShard.put(handled.shardId(), handled);
      } catch (Exception e) {
        log.error("Failed to checkpoint shard {}", handled.shardId(), e);
      }
    }
    latestHandledByShard.clear();
  }

  /**
   * Gives up every shard lease through the scheduler's graceful shutdown, so other workers take the
   * shards over; each shard processor checkpoints its last handled record on the way out. Queued
   * records were never handled and are left to the next owner.
   */
  @Override
  public void pause() {
    releasing.set(true);
    recordQueue.clear();
    Scheduler scheduler = schedulerRef.get();
    if (scheduler != null) {
      boolean graceful = false;
      try {
        graceful =
            scheduler
                .startGracefulShutdown()
                .get(GRACEFUL_SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        if (!graceful) {
          log.warn("Kinesis graceful shutdown did not complete; leases will expire instead");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        log.warn("Kinesis graceful shutdown failed; leases will expire instead", e);
      }
      if (!graceful) {
        scheduler.shutdown();
      }
      awaitSchedulerRunExit();
    }
    recordQueue.clear();
    progress.next(List.of());
    latestHandledByShard.clear();
    lastCheckpointedByShard.clear();
    drainedThroughEvent = new int[0];
    millisBehindByShard.clear();
  }

  @Override
  public void resume() {
    recordQueue.clear();
    awaitSchedulerRunExit();
    releasing.set(false);
    Scheduler scheduler = schedulerFactory.get();
    if (scheduler == null) {
      return;
    }
    schedulerRef.set(scheduler);
    started.set(false);
    start();
  }

  record ShardRecord(
      String shardId, RecordProcessorCheckpointer checkpointer, KinesisClientRecord record) {}

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

    private final BlockingQueue<ShardRecord> records;
    private final Map<String, Long> millisBehindByShard;
    private final AtomicBoolean releasing;
    private final Map<String, ShardRecord> lastCheckpointedByShard;

    private RecordProcessorFactory(
        BlockingQueue<ShardRecord> records,
        Map<String, Long> millisBehindByShard,
        AtomicBoolean releasing,
        Map<String, ShardRecord> lastCheckpointedByShard) {
      this.records = records;
      this.millisBehindByShard = millisBehindByShard;
      this.releasing = releasing;
      this.lastCheckpointedByShard = lastCheckpointedByShard;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor(StreamIdentifier streamIdentifier) {
      return shardRecordProcessor();
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
      return new ZephflowShardRecordProcessor(
          records, millisBehindByShard, releasing, lastCheckpointedByShard);
    }
  }

  @Slf4j
  public static class ZephflowShardRecordProcessor implements ShardRecordProcessor {

    private final BlockingQueue<ShardRecord> records;
    private final Map<String, Long> millisBehindByShard;
    private final AtomicBoolean releasing;
    private final Map<String, ShardRecord> lastCheckpointedByShard;
    private String shardId;

    private ZephflowShardRecordProcessor(
        BlockingQueue<ShardRecord> records,
        Map<String, Long> millisBehindByShard,
        AtomicBoolean releasing,
        Map<String, ShardRecord> lastCheckpointedByShard) {
      log.info("Starting ZephflowShardRecordProcessor processor");
      this.records = records;
      this.millisBehindByShard = millisBehindByShard;
      this.releasing = releasing;
      this.lastCheckpointedByShard = lastCheckpointedByShard;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
      log.info("Initializing KinesisSourceFetcher: {}", initializationInput.toString());
      shardId = initializationInput.shardId();
    }

    @SneakyThrows
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
      // Kinesis consumer uses a push model to get records, while the Zephflow source commands are
      // pull based.
      // To bridge this we push data to a blocking queue which Zephflow will pull from as it
      // processes the records.
      Long millisBehind = processRecordsInput.millisBehindLatest();
      if (millisBehind != null) {
        millisBehindByShard.put(shardId, millisBehind);
      }
      var checkpointer = processRecordsInput.checkpointer();
      for (var record : processRecordsInput.records()) {
        try {
          log.debug("Got record: {}", record);
          ShardRecord shardRecord = new ShardRecord(shardId, checkpointer, record);
          do {
            if (releasing.get()) {
              return;
            }
          } while (!records.offer(shardRecord, 100, TimeUnit.MILLISECONDS));
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
      millisBehindByShard.remove(shardId);
      lastCheckpointedByShard.remove(shardId);
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
      log.info("Shard ended: {}", shardEndedInput);
      millisBehindByShard.remove(shardId);
      lastCheckpointedByShard.remove(shardId);
      try {
        shardEndedInput.checkpointer().checkpoint();
      } catch (Exception e) {
        log.error("Error checkpointing at shard end: {}", e, e);
      }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
      // Records still queued here are not handled yet; only the last handled one is checkpointed.
      log.info("Shutdown requested: {}", shutdownRequestedInput);
      millisBehindByShard.remove(shardId);
      ShardRecord handled = lastCheckpointedByShard.get(shardId);
      if (handled == null) {
        return;
      }
      try {
        shutdownRequestedInput
            .checkpointer()
            .checkpoint(handled.record().sequenceNumber(), handled.record().subSequenceNumber());
      } catch (Exception e) {
        log.error("Failed to checkpoint shard {} on shutdown", shardId, e);
      }
    }
  }
}
