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

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendConfig;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointClient;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.FsCheckpoint;
import io.fleak.zephflow.lib.commands.fssource.util.SourceIdHasher;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.lib.utils.CompressionUtils;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

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
    return executionContext;
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
        SourceIdHasher.compute(config.getBackend(), config.getRoot(), config.getFileNameRegex());
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
      try {
        byte[] bytes;
        try (InputStream inputStream = executionContext.reader.open(fileEntry.key(), 0)) {
          bytes = maybeGunzip(inputStream.readAllBytes());
        }
        List<RecordFleakData> records =
            deserializer.deserialize(new SerializedEvent(null, bytes, null));
        eventAcceptor.accept(records);
        currentCheckpoint =
            currentCheckpoint.withEmitted(fileEntry.key().urn(), pending.timestamp());
        saveCheckpoint(executionContext.checkpointClient, sourceId, currentCheckpoint);
      } catch (Exception exception) {
        log.error(
            "fs_source skip file urn={} due to read/deserialize error",
            fileEntry.key().urn(),
            exception);
      }
    }
    eventAcceptor.terminate();
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
