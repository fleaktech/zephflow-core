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
package io.fleak.zephflow.lib.commands.fssource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointClient;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.FsCheckpoint;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.FsCheckpointStore;
import io.fleak.zephflow.lib.commands.fssource.util.Partitioner;
import io.fleak.zephflow.lib.commands.fssource.util.PendingFile;
import io.fleak.zephflow.lib.commands.fssource.util.PendingFileSelector;
import io.fleak.zephflow.lib.commands.fssource.util.SourceIdHasher;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializationOutcome;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
public final class FsSourceCommand extends SourceCommand {

  static final String DEFAULT_CHECKPOINT_SCOPE = "local";
  static final String SKIP_REASON_READ_ERROR = "read_error";
  static final String SKIP_REASON_DOWNSTREAM_ERROR = "downstream_error";
  static final String SKIP_REASON_NOTHING_DESERIALIZED = "nothing_deserialized";
  static final long DEFAULT_MAX_FILE_BYTES = 256L * 1024 * 1024;
  static final int DEFAULT_CHUNK_SIZE_BYTES = 16 * 1024 * 1024;
  static final int DEFAULT_MAX_FILES_PER_RUN = 10_000;
  static final String SKIP_REASON_FILE_TOO_LARGE = "file_too_large";

  private volatile boolean terminated = false;

  public FsSourceCommand(String nodeId, JobContext jobContext) {
    super(nodeId, jobContext, new FsSourceConfigParser(), new FsSourceConfigValidator());
  }

  @Override
  public String commandName() {
    return "fssource";
  }

  @Override
  public SourceType sourceType() {
    return SourceType.BATCH;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    FsSourceDto.Config config = (FsSourceDto.Config) commandConfig;
    FsSourceExecutionContext executionContext = new FsSourceExecutionContext();
    executionContext.backend = FsBackendRegistry.get(config.getBackend());
    FsBackendConfig backendConfig = buildBackendConfig(config, jobContext);
    executionContext.backendConfig = backendConfig;
    executionContext.lister = executionContext.backend.createLister(backendConfig);
    executionContext.reader = executionContext.backend.createReader(backendConfig);
    executionContext.payloadReader =
        new FsPayloadReader(
            executionContext.reader,
            config.getMaxFileBytes() == null ? DEFAULT_MAX_FILE_BYTES : config.getMaxFileBytes(),
            config.getChunkSizeBytes() == null
                ? DEFAULT_CHUNK_SIZE_BYTES
                : config.getChunkSizeBytes());
    executionContext.checkpointClient = buildCheckpointClient(jobContext);
    executionContext.checkpointScope = checkpointScope(jobContext);
    executionContext.replicaIndex = parseIntProperty(jobContext, JobContext.REPLICA_INDEX, 0);
    executionContext.replicaCount = parseIntProperty(jobContext, JobContext.REPLICA_COUNT, 1);

    Map<String, String> metricTags = metricTags(jobContext, nodeId);
    executionContext.dataSizeCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_SIZE_COUNT, metricTags);
    executionContext.inputEventCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    executionContext.deserializeFailureCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_DESER_ERR_COUNT, metricTags);
    executionContext.skippedFileCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_FILE_SKIPPED_COUNT, metricTags);
    executionContext.dlqWriter = buildDlqWriter(jobContext);
    return executionContext;
  }

  /**
   * Same shape as {@link io.fleak.zephflow.lib.utils.MiscUtils#basicCommandMetricTags}, minus its
   * precondition on service/env tags: a batch file read must still run when the job wasn't given
   * metric tags.
   */
  private Map<String, String> metricTags(JobContext jobContext, String nodeId) {
    Map<String, String> metricTags =
        new java.util.HashMap<>(
            jobContext.getMetricTags() == null ? Map.of() : jobContext.getMetricTags());
    metricTags.put(METRIC_TAG_COMMAND_NAME, commandName());
    metricTags.put(METRIC_TAG_NODE_ID, nodeId);
    return metricTags;
  }

  private static DlqWriter buildDlqWriter(JobContext jobContext) {
    JobContext.DlqConfig dlqConfig = jobContext.getDlqConfig();
    if (dlqConfig == null) {
      return null;
    }
    String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
    DlqWriter dlqWriter = DlqWriterFactory.createDlqWriter(dlqConfig, keyPrefix);
    dlqWriter.open();
    return dlqWriter;
  }

  private static int parseIntProperty(JobContext jobContext, String key, int defaultValue) {
    Object value = jobContext.getOtherProperties().get(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value.toString().trim());
    } catch (NumberFormatException numberFormatException) {
      log.warn("fs_source: unparseable {}={}; using default {}", key, value, defaultValue);
      return defaultValue;
    }
  }

  /**
   * Resolves the identity the resume checkpoint belongs to. The scheduler-provided {@link
   * JobContext#CHECKPOINT_SCOPE} keeps each workflow's progress separate over a shared folder;
   * without it the checkpoint falls back to the current job, so a run re-reads the folder rather
   * than inheriting another workflow's progress.
   */
  static String checkpointScope(JobContext jobContext) {
    Object explicitScope =
        jobContext.getOtherProperties() == null
            ? null
            : jobContext.getOtherProperties().get(JobContext.CHECKPOINT_SCOPE);
    if (explicitScope != null && !explicitScope.toString().isBlank()) {
      return explicitScope.toString().trim();
    }
    String jobId =
        jobContext.getMetricTags() == null
            ? null
            : jobContext.getMetricTags().get(METRIC_TAG_JOB_ID);
    if (jobId != null && !jobId.isBlank()) {
      log.warn(
          "fs_source: no {} in the job context; scoping the resume checkpoint to job_id={}, so this"
              + " job cannot resume a previous job's progress",
          JobContext.CHECKPOINT_SCOPE,
          jobId);
      return jobId.trim();
    }
    log.debug(
        "fs_source: no {} and no {} in the job context; using checkpoint scope {}",
        JobContext.CHECKPOINT_SCOPE,
        METRIC_TAG_JOB_ID,
        DEFAULT_CHECKPOINT_SCOPE);
    return DEFAULT_CHECKPOINT_SCOPE;
  }

  private static CheckpointClient buildCheckpointClient(JobContext jobContext) {
    Object url = jobContext.getOtherProperties().get(JobContext.CHECKPOINT_URL);
    String trimmedUrl = url == null ? null : url.toString().trim();
    if (trimmedUrl == null || trimmedUrl.isEmpty()) {
      return new CheckpointClient.InMemCheckpointClient();
    }
    return new CheckpointClient.HttpCheckpointClient(trimmedUrl);
  }

  private static FsBackendConfig buildBackendConfig(
      FsSourceDto.Config config, JobContext jobContext) {
    return switch (config.getBackend()) {
      case "file" -> new LocalFsBackendConfig(config.getRoot());
      case "s3" -> s3BackendConfig(config.getBackendConfig(), jobContext);
      case "gs" -> gcsBackendConfig(config.getBackendConfig());
      case "azblob" -> azureBackendConfig(config.getBackendConfig(), jobContext);
      case "sftp" ->
          SftpBackendConfig.from(config.getRoot(), config.getBackendConfig(), jobContext);
      default -> throw new IllegalArgumentException("Unsupported backend: " + config.getBackend());
    };
  }

  private static S3BackendConfig s3BackendConfig(
      java.util.Map<String, Object> backendConfigMap, JobContext jobContext) {
    if (backendConfigMap == null) backendConfigMap = java.util.Map.of();
    String region = (String) backendConfigMap.getOrDefault("region", "us-east-1");
    String credentialId = (String) backendConfigMap.get("credentialId");
    String endpoint = (String) backendConfigMap.get("s3EndpointOverride");
    io.fleak.zephflow.lib.credentials.UsernamePasswordCredential credential =
        io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredentialOpt(
                jobContext, credentialId)
            .orElse(null);
    if (credentialId != null && !credentialId.isBlank() && credential == null) {
      throw new IllegalStateException(
          "S3 credentialId '"
              + credentialId
              + "' was configured but could not be resolved in JobContext");
    }
    String accessKeyId = credential != null ? credential.getUsername() : null;
    String secretAccessKey = credential != null ? credential.getPassword() : null;
    return new S3BackendConfig(region, accessKeyId, secretAccessKey, endpoint);
  }

  private static GcsBackendConfig gcsBackendConfig(java.util.Map<String, Object> backendConfigMap) {
    if (backendConfigMap == null) backendConfigMap = java.util.Map.of();
    String serviceAccountJson = (String) backendConfigMap.get("serviceAccountJson");
    return new GcsBackendConfig(serviceAccountJson);
  }

  private static AzureBackendConfig azureBackendConfig(
      java.util.Map<String, Object> backendConfigMap, JobContext jobContext) {
    if (backendConfigMap == null) backendConfigMap = java.util.Map.of();
    String connectionString = (String) backendConfigMap.get("connectionString");
    if (connectionString != null && !connectionString.isBlank()) {
      return new AzureBackendConfig(connectionString, null, null);
    }
    String credentialId = (String) backendConfigMap.get("credentialId");
    if (credentialId != null && !credentialId.isBlank()) {
      io.fleak.zephflow.lib.credentials.UsernamePasswordCredential credential =
          io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential(
              jobContext, credentialId);
      return new AzureBackendConfig(null, credential.getUsername(), credential.getPassword());
    }
    throw new IllegalArgumentException(
        "azblob backend requires either 'connectionString' or 'credentialId' in backendConfig");
  }

  @Override
  public void execute(String user, SourceEventAcceptor eventAcceptor) throws Exception {
    FsSourceExecutionContext executionContext = (FsSourceExecutionContext) getExecutionContext();
    FsSourceDto.Config config = (FsSourceDto.Config) commandConfig;

    List<Integer> ownedBuckets =
        Partitioner.ownedBuckets(executionContext.replicaIndex, executionContext.replicaCount);
    Map<Integer, String> sourceIdByBucket = new HashMap<>();
    Map<Integer, FsCheckpoint> checkpointByBucket = new HashMap<>();
    for (int bucket : ownedBuckets) {
      String bucketSourceId =
          SourceIdHasher.compute(
              executionContext.checkpointScope,
              nodeId,
              config.getBackend(),
              config.getRoot(),
              config.getFileNameRegex(),
              config.getExactObjectKey(),
              bucket);
      sourceIdByBucket.put(bucket, bucketSourceId);
      checkpointByBucket.put(
          bucket, FsCheckpointStore.load(executionContext.checkpointClient, bucketSourceId));
    }

    log.info(
        "fs_source open: checkpointScope={} replica={}/{} buckets={}",
        executionContext.checkpointScope,
        executionContext.replicaIndex,
        executionContext.replicaCount,
        ownedBuckets.size());

    Pattern fileNamePattern =
        config.getFileNameRegex() == null ? null : Pattern.compile(config.getFileNameRegex());
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();

    ListRequest listRequest =
        new ListRequest(config.getRoot(), fileNamePattern, config.getExactObjectKey());
    PendingFileSelector selector =
        new PendingFileSelector(
            config.getMaxFilesPerRun() == null
                ? DEFAULT_MAX_FILES_PER_RUN
                : config.getMaxFilesPerRun());
    long[] listedCount = {0};
    try (var stream = executionContext.lister.list(listRequest)) {
      stream
          // Counted before any filter: this is whether the root matched anything at all, not
          // whether anything was left to do. A run that lists files and filters every one of them
          // out as already-completed is the normal idle case and must stay silent.
          .peek(fileEntry -> listedCount[0]++)
          .map(
              fileEntry ->
                  new PendingFile(fileEntry, timestampFromName(fileEntry, fileNamePattern)))
          .filter(pending -> checkpointByBucket.containsKey(bucketOf(pending)))
          // Files older than their bucket's resume watermark are intentionally skipped.
          .filter(
              pending ->
                  pending
                          .timestamp()
                          .compareTo(checkpointByBucket.get(bucketOf(pending)).watermark())
                      >= 0)
          .filter(
              pending ->
                  !checkpointByBucket
                      .get(bucketOf(pending))
                      .isCompleted(pending.entry().key().urn()))
          .forEach(selector::offer);
    }
    if (listedCount[0] == 0) {
      log.warn(
          "fs_source: listing root={} matched no objects; check the root prefix and any"
              + " fileNameRegex/exactObjectKey filters",
          config.getRoot());
    }
    List<PendingFile> pendingFiles = selector.oldestFirst();
    if (selector.capped()) {
      log.info(
          "fs_source: listing exceeded maxFilesPerRun; processing the oldest {} file(s), the rest"
              + " follow on the next run",
          pendingFiles.size());
    }

    Map<Integer, Instant> ceilingByBucket = new HashMap<>();
    int emittedFileCount = 0;
    int skippedFileCount = 0;
    int abandonedFileCount = 0;

    for (PendingFile pending : pendingFiles) {
      if (terminated) break;
      FileEntry fileEntry = pending.entry();
      int bucket = bucketOf(pending);

      String skipReason = readAndEmit(executionContext, deserializer, eventAcceptor, fileEntry);
      if (SKIP_REASON_FILE_TOO_LARGE.equals(skipReason)) {
        // Unlike the other skips, a retry reads the same bytes against the same cap and fails the
        // same way. Holding the watermark for it would re-read it every run and pin the bucket
        // forever, so it is checkpointed as done: the file is dropped, not retried.
        countSkip(executionContext, skipReason);
        abandonedFileCount++;
      } else if (skipReason != null) {
        ceilingByBucket.merge(bucket, pending.timestamp(), FsSourceCommand::holdWatermark);
        skippedFileCount += countSkip(executionContext, skipReason);
        continue;
      } else {
        emittedFileCount++;
      }

      // Records that did parse were emitted, and malformed ones were quarantined, so the file is
      // done. Checkpointing is what keeps a retry from re-emitting the records already emitted.
      FsCheckpoint updated =
          checkpointByBucket
              .get(bucket)
              .withEmitted(
                  fileEntry.key().urn(),
                  pending.timestamp(),
                  ceilingByBucket.getOrDefault(bucket, Instant.MAX));
      checkpointByBucket.put(bucket, updated);
      FsCheckpointStore.save(
          executionContext.checkpointClient, sourceIdByBucket.get(bucket), updated);
    }

    if (abandonedFileCount > 0) {
      log.error(
          "fs_source run summary: scope={} replica={}/{} dropped {} file(s) over maxFileBytes;"
              + " they will not be retried. See the preceding {} errors",
          executionContext.checkpointScope,
          executionContext.replicaIndex,
          executionContext.replicaCount,
          abandonedFileCount,
          SKIP_REASON_FILE_TOO_LARGE);
    }
    if (skippedFileCount > 0) {
      // ceilingByBucket holds one entry per bucket that had an unresolved file this run: that
      // bucket's watermark cannot advance past it, so a file that can never be resolved pins it
      // forever and completedSinceWatermark stops pruning. There's no longer a single ceiling to
      // report (it's per bucket), so report its shape: how many buckets are stuck, and how old the
      // oldest stuck one is.
      Optional<Instant> earliestHeldWatermark =
          ceilingByBucket.values().stream().min(Instant::compareTo);
      log.warn(
          "fs_source run summary: scope={} replica={}/{} emitted={} skipped={} heldWatermarkBuckets={}"
              + " earliestHeldWatermark={}",
          executionContext.checkpointScope,
          executionContext.replicaIndex,
          executionContext.replicaCount,
          emittedFileCount,
          skippedFileCount,
          ceilingByBucket.size(),
          earliestHeldWatermark.map(Instant::toString).orElse("n/a"));
    }
    if (emittedFileCount == 0 && skippedFileCount > 0) {
      // Reporting success here would tell the scheduler the batch is done when nothing was read.
      // Dropped oversized files don't count: they are done, and a retry would find nothing to do.
      throw new IllegalStateException(
          "fs_source read no files: all "
              + skippedFileCount
              + " candidate file(s) were skipped; see the preceding errors");
    }
    eventAcceptor.terminate();
  }

  /**
   * Reads one file and emits its records.
   *
   * @return null when the file is fully handled, or the skip reason when it is not
   */
  private String readAndEmit(
      FsSourceExecutionContext executionContext,
      FleakDeserializer<?> deserializer,
      SourceEventAcceptor eventAcceptor,
      FileEntry fileEntry) {
    String urn = fileEntry.key().urn();
    try {
      if (deserializer.supportsStreamedPayloads()) {
        return emitStreamed(executionContext, deserializer, eventAcceptor, fileEntry);
      }
      return deserializer.supportsChunkedPayloads()
          ? emitChunked(executionContext, deserializer, eventAcceptor, fileEntry)
          : emitWholePayload(executionContext, deserializer, eventAcceptor, fileEntry);
    } catch (FsPayloadReader.PayloadTooLargeException payloadTooLargeException) {
      // Size is the single most common reason a file is skipped, and the one an operator can act
      // on directly, so name the reason and the listed object size rather than only the exception.
      log.error(
          "fs_source drop file urn={} reason={} listedSizeBytes={}: {}. It is checkpointed as"
              + " done and will not be retried; any records already emitted from it stand",
          urn,
          SKIP_REASON_FILE_TOO_LARGE,
          fileEntry.size(),
          payloadTooLargeException.getMessage());
      return SKIP_REASON_FILE_TOO_LARGE;
    } catch (DownstreamFailure downstreamFailure) {
      log.error(
          "fs_source skip file urn={} due to downstream error", urn, downstreamFailure.getCause());
      return SKIP_REASON_DOWNSTREAM_ERROR;
    } catch (Exception exception) {
      log.error("fs_source skip file urn={} due to read error", urn, exception);
      return SKIP_REASON_READ_ERROR;
    }
  }

  /** Whole-document formats: one capped read, one parse, one emit. */
  private String emitWholePayload(
      FsSourceExecutionContext executionContext,
      FleakDeserializer<?> deserializer,
      SourceEventAcceptor eventAcceptor,
      FileEntry fileEntry)
      throws Exception {
    String urn = fileEntry.key().urn();
    byte[] bytes = executionContext.payloadReader.readWhole(fileEntry.key());
    executionContext.dataSizeCounter.increase(bytes.length, Map.of());

    DeserializationOutcome outcome =
        deserializer.deserializeWithErrors(new SerializedEvent(null, bytes, Map.of()));
    boolean quarantined = reportDeserializationErrors(executionContext, urn, outcome);
    emit(executionContext, eventAcceptor, outcome);

    if (outcome.records().isEmpty() && !quarantined) {
      log.error("fs_source skip file urn={}: nothing could be deserialized", urn);
      return SKIP_REASON_NOTHING_DESERIALIZED;
    }
    return null;
  }

  /** Line-delimited formats: parse and emit one newline-aligned chunk at a time. */
  private String emitChunked(
      FsSourceExecutionContext executionContext,
      FleakDeserializer<?> deserializer,
      SourceEventAcceptor eventAcceptor,
      FileEntry fileEntry)
      throws Exception {
    String urn = fileEntry.key().urn();
    // Arrays because the lambda needs to mutate them; the consumer is called on this thread only.
    long[] recordCount = {0};
    boolean[] unrecordedFailures = {false};

    executionContext.payloadReader.forEachChunk(
        fileEntry.key(),
        chunk -> {
          executionContext.dataSizeCounter.increase(chunk.length, Map.of());
          DeserializationOutcome outcome =
              deserializer.deserializeWithErrors(new SerializedEvent(null, chunk, Map.of()));
          // Unconditional: it returns true (nothing to record) when the chunk had no failures.
          if (!reportDeserializationErrors(executionContext, urn, outcome)) {
            unrecordedFailures[0] = true;
          }
          recordCount[0] += outcome.records().size();
          emit(executionContext, eventAcceptor, outcome);
        });

    if (recordCount[0] == 0 && unrecordedFailures[0]) {
      log.error("fs_source skip file urn={}: nothing could be deserialized", urn);
      return SKIP_REASON_NOTHING_DESERIALIZED;
    }
    return null;
  }

  /**
   * Record-sequence formats that are not line-delimited (json array, csv): the parser reads records
   * straight from the stream, and they are emitted in batches of about {@code chunkSizeBytes}, so
   * neither the file nor the parsed records are ever held whole.
   */
  private String emitStreamed(
      FsSourceExecutionContext executionContext,
      FleakDeserializer<?> deserializer,
      SourceEventAcceptor eventAcceptor,
      FileEntry fileEntry)
      throws Exception {
    String urn = fileEntry.key().urn();
    int batchSizeBytes = executionContext.payloadReader.chunkSizeBytes();
    // Arrays because the lambdas need to mutate them; everything here runs on this thread.
    long[] recordCount = {0};
    boolean[] unrecordedFailures = {false};
    long[] flushedThroughBytes = {0};
    List<RecordFleakData> records = new ArrayList<>();
    List<DeserializationOutcome.RecordError> errors = new ArrayList<>();

    executionContext.payloadReader.stream(
        fileEntry.key(),
        input -> {
          Runnable flush =
              () -> {
                executionContext.dataSizeCounter.increase(
                    input.bytesRead() - flushedThroughBytes[0], Map.of());
                flushedThroughBytes[0] = input.bytesRead();
                DeserializationOutcome outcome =
                    new DeserializationOutcome(List.copyOf(records), List.copyOf(errors));
                records.clear();
                errors.clear();
                if (!reportDeserializationErrors(executionContext, urn, outcome)) {
                  unrecordedFailures[0] = true;
                }
                recordCount[0] += outcome.records().size();
                emit(executionContext, eventAcceptor, outcome);
              };
          Runnable flushWhenFull =
              () -> {
                if (input.bytesRead() - flushedThroughBytes[0] >= batchSizeBytes) {
                  flush.run();
                }
              };
          try {
            deserializer.deserializeStream(
                input,
                record -> {
                  input.recordBoundary();
                  records.add(record);
                  flushWhenFull.run();
                },
                error -> {
                  input.recordBoundary();
                  errors.add(error);
                  flushWhenFull.run();
                });
          } catch (FsPayloadReader.PayloadTooLargeException payloadTooLargeException) {
            // The file is dropped rather than retried, so the records parsed before the oversized
            // one must still go out, or they are lost along with it.
            flush.run();
            throw payloadTooLargeException;
          }
          flush.run();
        });

    if (recordCount[0] == 0 && unrecordedFailures[0]) {
      log.error("fs_source skip file urn={}: nothing could be deserialized", urn);
      return SKIP_REASON_NOTHING_DESERIALIZED;
    }
    return null;
  }

  /**
   * Emits one batch, tagging a downstream failure so the caller can tell it from a read failure.
   */
  private static void emit(
      FsSourceExecutionContext executionContext,
      SourceEventAcceptor eventAcceptor,
      DeserializationOutcome outcome) {
    if (outcome.records().isEmpty()) {
      return;
    }
    executionContext.inputEventCounter.increase(outcome.records().size(), Map.of());
    try {
      eventAcceptor.accept(outcome.records());
    } catch (Exception exception) {
      throw new DownstreamFailure(exception);
    }
  }

  /** A failure from the downstream DAG rather than from reading or parsing the file. */
  private static class DownstreamFailure extends RuntimeException {
    DownstreamFailure(Exception cause) {
      super(cause);
    }
  }

  /**
   * Counts and logs deserialization failures, and writes the offending raw records to the dlq when
   * one is configured.
   *
   * @return whether every failure is now recorded somewhere durable (trivially true when there were
   *     no failures)
   */
  private boolean reportDeserializationErrors(
      FsSourceExecutionContext executionContext, String urn, DeserializationOutcome outcome) {
    if (!outcome.hasErrors()) {
      return true;
    }
    executionContext.deserializeFailureCounter.increase(outcome.errors().size(), Map.of());
    log.error(
        "fs_source urn={}: {} record(s) failed to deserialize, {} emitted. first failure: {}",
        urn,
        outcome.errors().size(),
        outcome.records().size(),
        outcome.errors().getFirst().error().toString());
    if (executionContext.dlqWriter == null) {
      return false;
    }
    for (DeserializationOutcome.RecordError recordError : outcome.errors()) {
      Map<String, String> metadata = new java.util.HashMap<>();
      metadata.put(METADATA_FS_SOURCE_URN, urn);
      if (recordError.recordIndex() > 0) {
        metadata.put(METADATA_FS_SOURCE_RECORD_INDEX, String.valueOf(recordError.recordIndex()));
      }
      executionContext.dlqWriter.writeToDlq(
          System.currentTimeMillis(),
          new SerializedEvent(null, recordError.rawRecord(), metadata),
          ExceptionUtils.getStackTrace(recordError.error()),
          nodeId);
    }
    return true;
  }

  /**
   * Lowers the watermark ceiling to {@code timestamp} when it is older. Files are processed oldest
   * first, so the first skip already carries the oldest unresolved timestamp, but taking the
   * minimum keeps this correct regardless of iteration order.
   */
  private static Instant holdWatermark(Instant watermarkCeiling, Instant timestamp) {
    return timestamp.isBefore(watermarkCeiling) ? timestamp : watermarkCeiling;
  }

  private static int bucketOf(PendingFile pending) {
    return Partitioner.virtualBucket(pending.entry().key().urn());
  }

  /** Counts one skipped file against {@code reason} and returns 1, for the caller's tally. */
  private static int countSkip(FsSourceExecutionContext executionContext, String reason) {
    executionContext.skippedFileCounter.increase(1, Map.of(METRIC_TAG_SKIP_REASON, reason));
    return 1;
  }

  @Override
  public void terminate() throws java.io.IOException {
    terminated = true;
    super.terminate();
  }

  private static Instant timestampFromName(FileEntry fileEntry, Pattern fileNamePattern) {
    if (fileNamePattern == null) return fileEntry.lastModified();
    String name = new java.io.File(fileEntry.displayPath()).getName();
    Matcher matcher = fileNamePattern.matcher(name);
    if (!matcher.matches()) return fileEntry.lastModified();
    try {
      String timestamp = matcher.group("ts");
      return Instant.ofEpochSecond(Long.parseLong(timestamp));
    } catch (Exception exception) {
      return fileEntry.lastModified();
    }
  }
}
