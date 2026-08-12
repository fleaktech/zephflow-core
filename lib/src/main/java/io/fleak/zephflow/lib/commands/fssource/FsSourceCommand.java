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
import io.fleak.zephflow.lib.commands.fssource.api.*;
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointClient;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.FsCheckpoint;
import io.fleak.zephflow.lib.commands.fssource.util.Partitioner;
import io.fleak.zephflow.lib.commands.fssource.util.SourceIdHasher;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializationOutcome;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.lib.utils.CompressionUtils;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

@Slf4j
public final class FsSourceCommand extends SourceCommand {

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
    executionContext.checkpointClient = buildCheckpointClient(jobContext);
    executionContext.replicaIndex = parseIntProperty(jobContext, JobContext.REPLICA_INDEX, 0);
    executionContext.replicaCount = parseIntProperty(jobContext, JobContext.REPLICA_COUNT, 1);

    Map<String, String> metricTags = metricTags(jobContext, nodeId);
    executionContext.dataSizeCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_SIZE_COUNT, metricTags);
    executionContext.inputEventCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    executionContext.deserializeFailureCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_DESER_ERR_COUNT, metricTags);
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

    String sourceId =
        SourceIdHasher.compute(
            config.getBackend(),
            config.getRoot(),
            config.getFileNameRegex(),
            executionContext.replicaIndex,
            executionContext.replicaCount);
    FsCheckpoint checkpoint = loadCheckpoint(executionContext.checkpointClient, sourceId);
    log.info("fs_source open: sourceId={} watermark={}", sourceId, checkpoint.watermark());

    Pattern fileNamePattern =
        config.getFileNameRegex() == null ? null : Pattern.compile(config.getFileNameRegex());
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();

    ListRequest listRequest = new ListRequest(config.getRoot(), fileNamePattern);
    List<Pending> pendingFiles = new ArrayList<>();
    try (var stream = executionContext.lister.list(listRequest)) {
      stream
          .map(fileEntry -> new Pending(fileEntry, timestampFromName(fileEntry, fileNamePattern)))
          .filter(
              pending ->
                  Partitioner.owns(
                      pending.entry().key().urn(),
                      executionContext.replicaIndex,
                      executionContext.replicaCount))
          // Files older than the resume watermark are intentionally skipped on later runs.
          .filter(pending -> pending.timestamp().compareTo(checkpoint.watermark()) >= 0)
          .filter(pending -> !checkpoint.isCompleted(pending.entry().key().urn()))
          .sorted(
              Comparator.comparing(Pending::timestamp)
                  .thenComparing(pending -> pending.entry().key().urn()))
          .forEach(pendingFiles::add);
    }

    FsCheckpoint currentCheckpoint = checkpoint;
    for (Pending pending : pendingFiles) {
      if (terminated) break;
      FileEntry fileEntry = pending.entry();
      String urn = fileEntry.key().urn();

      byte[] bytes;
      try (InputStream inputStream = executionContext.reader.open(fileEntry.key(), 0)) {
        bytes = maybeGunzip(inputStream.readAllBytes());
      } catch (Exception exception) {
        // Transient: leave the file uncheckpointed so a later run retries it.
        log.error("fs_source skip file urn={} due to read error", urn, exception);
        continue;
      }
      executionContext.dataSizeCounter.increase(bytes.length, Map.of());

      DeserializationOutcome outcome =
          deserializer.deserializeWithErrors(new SerializedEvent(null, bytes, Map.of()));
      boolean quarantined = reportDeserializationErrors(executionContext, urn, outcome);

      try {
        if (!outcome.records().isEmpty()) {
          executionContext.inputEventCounter.increase(outcome.records().size(), Map.of());
          eventAcceptor.accept(outcome.records());
        }
      } catch (Exception exception) {
        // Downstream failure, not a data problem: don't checkpoint, so the file is retried.
        log.error("fs_source skip file urn={} due to downstream error", urn, exception);
        continue;
      }

      if (outcome.records().isEmpty() && !quarantined) {
        // Nothing parsed and nowhere to quarantine it: leave uncheckpointed so a retry is possible.
        log.error("fs_source skip file urn={}: nothing could be deserialized", urn);
        continue;
      }

      // Records that did parse were emitted, and malformed ones were quarantined, so the file is
      // done. Checkpointing is what keeps a retry from re-emitting the records already emitted.
      currentCheckpoint = currentCheckpoint.withEmitted(urn, pending.timestamp());
      saveCheckpoint(executionContext.checkpointClient, sourceId, currentCheckpoint);
    }
    eventAcceptor.terminate();
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

  private static FsCheckpoint loadCheckpoint(CheckpointClient checkpointClient, String sourceId) {
    return checkpointClient
        .loadCheckpoint(sourceId)
        .map(checkpointData -> JsonUtils.fromJsonString(checkpointData.data(), FsCheckpoint.class))
        .orElse(FsCheckpoint.empty());
  }

  private static void saveCheckpoint(
      CheckpointClient checkpointClient, String sourceId, FsCheckpoint checkpoint) {
    checkpointClient.checkpoint(sourceId, JsonUtils.toJsonString(checkpoint));
  }

  /** Auto-detect gzip by magic bytes (0x1f 0x8b) and decompress; otherwise pass through. */
  static byte[] maybeGunzip(byte[] data) {
    if (data.length >= 2 && (data[0] & 0xff) == 0x1f && (data[1] & 0xff) == 0x8b) {
      return CompressionUtils.gunzip(data);
    }
    return data;
  }

  private record Pending(FileEntry entry, Instant timestamp) {}

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
