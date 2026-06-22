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
      MetricClientProvider mp, JobContext jc, CommandConfig cfg, String nodeId) {
    FsSourceDto.Config c = (FsSourceDto.Config) cfg;
    FsSourceExecutionContext ec = new FsSourceExecutionContext();
    ec.backend = FsBackendRegistry.get(c.getBackend());
    FsBackendConfig bc = buildBackendConfig(c, jc);
    ec.backendConfig = bc;
    ec.lister = ec.backend.createLister(bc);
    ec.reader = ec.backend.createReader(bc);
    ec.checkpointClient = buildCheckpointClient(jc);
    return ec;
  }

  private static CheckpointClient buildCheckpointClient(JobContext jc) {
    Object url = jc.getOtherProperties().get(JobContext.JOB_MASTER_URL);
    String s = url == null ? null : url.toString().trim();
    if (s == null || s.isEmpty()) {
      return new CheckpointClient.InMemCheckpointClient();
    }
    return new CheckpointClient.HttpCheckpointClient(s);
  }

  private static FsBackendConfig buildBackendConfig(FsSourceDto.Config c, JobContext jobContext) {
    return switch (c.getBackend()) {
      case "file" -> new LocalFsBackendConfig(c.getRoot());
      case "s3" -> s3BackendConfig(c.getBackendConfig());
      case "gs" -> gcsBackendConfig(c.getBackendConfig());
      case "azblob" -> azureBackendConfig(c.getBackendConfig(), jobContext);
      default -> throw new IllegalArgumentException("Unsupported backend: " + c.getBackend());
    };
  }

  private static S3BackendConfig s3BackendConfig(java.util.Map<String, Object> map) {
    if (map == null) map = java.util.Map.of();
    String region = (String) map.getOrDefault("region", "us-east-1");
    String credentialId = (String) map.get("credentialId");
    String endpoint = (String) map.get("s3EndpointOverride");
    return new S3BackendConfig(region, credentialId, endpoint);
  }

  private static GcsBackendConfig gcsBackendConfig(java.util.Map<String, Object> map) {
    if (map == null) map = java.util.Map.of();
    String serviceAccountJson = (String) map.get("serviceAccountJson");
    return new GcsBackendConfig(serviceAccountJson);
  }

  private static AzureBackendConfig azureBackendConfig(
      java.util.Map<String, Object> map, JobContext jobContext) {
    if (map == null) map = java.util.Map.of();
    String connectionString = (String) map.get("connectionString");
    if (connectionString != null && !connectionString.isBlank()) {
      return new AzureBackendConfig(connectionString, null, null);
    }
    String credentialId = (String) map.get("credentialId");
    if (credentialId != null && !credentialId.isBlank()) {
      io.fleak.zephflow.lib.credentials.UsernamePasswordCredential cred =
          io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential(
              jobContext, credentialId);
      return new AzureBackendConfig(null, cred.getUsername(), cred.getPassword());
    }
    throw new IllegalArgumentException(
        "azblob backend requires either 'connectionString' or 'credentialId' in backendConfig");
  }

  @Override
  public void execute(String user, SourceEventAcceptor out) throws Exception {
    FsSourceExecutionContext ec = (FsSourceExecutionContext) getExecutionContext();
    FsSourceDto.Config c = (FsSourceDto.Config) commandConfig;

    String sourceId = SourceIdHasher.compute(c.getBackend(), c.getRoot(), c.getFileNameRegex());
    FsCheckpoint checkpoint = loadCheckpoint(ec.checkpointClient, sourceId);
    log.info("fs_source open: sourceId={} watermark={}", sourceId, checkpoint.watermark());

    Pattern regex = c.getFileNameRegex() == null ? null : Pattern.compile(c.getFileNameRegex());
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(c.getEncodingType()).createDeserializer();

    ListRequest req = new ListRequest(c.getRoot(), regex);
    List<Pending> todo = new ArrayList<>();
    try (var stream = ec.lister.list(req)) {
      stream
          .map(f -> new Pending(f, tsFromName(f, regex)))
          .filter(p -> p.ts().compareTo(checkpoint.watermark()) >= 0)
          .filter(p -> !checkpoint.isCompleted(p.entry().key().urn()))
          .sorted(Comparator.comparing(Pending::ts).thenComparing(p -> p.entry().key().urn()))
          .forEach(todo::add);
    }

    FsCheckpoint current = checkpoint;
    for (Pending p : todo) {
      if (terminated) break;
      FileEntry f = p.entry();
      byte[] bytes;
      try (InputStream in = ec.reader.open(f.key(), 0)) {
        bytes = maybeGunzip(in.readAllBytes());
      }
      List<RecordFleakData> records =
          deserializer.deserialize(new SerializedEvent(null, bytes, null));
      out.accept(records);
      current = current.withEmitted(f.key().urn(), p.ts());
      saveCheckpoint(ec.checkpointClient, sourceId, current);
    }
    out.terminate();
  }

  private static FsCheckpoint loadCheckpoint(CheckpointClient client, String sourceId) {
    return client
        .loadCheckpoint(sourceId)
        .map(d -> JsonUtils.fromJsonString(d.data(), FsCheckpoint.class))
        .orElse(FsCheckpoint.empty());
  }

  private static void saveCheckpoint(CheckpointClient client, String sourceId, FsCheckpoint cp) {
    client.checkpoint(sourceId, JsonUtils.toJsonString(cp));
  }

  /** Auto-detect gzip by magic bytes (0x1f 0x8b) and decompress; otherwise pass through. */
  static byte[] maybeGunzip(byte[] data) {
    if (data.length >= 2 && (data[0] & 0xff) == 0x1f && (data[1] & 0xff) == 0x8b) {
      return CompressionUtils.gunzip(data);
    }
    return data;
  }

  private record Pending(FileEntry entry, Instant ts) {}

  @Override
  public void terminate() throws java.io.IOException {
    terminated = true;
    super.terminate();
  }

  private static Instant tsFromName(FileEntry f, Pattern regex) {
    if (regex == null) return f.lastModified();
    String name = new java.io.File(f.displayPath()).getName();
    Matcher m = regex.matcher(name);
    if (!m.matches()) return f.lastModified();
    try {
      String ts = m.group("ts");
      return Instant.ofEpochSecond(Long.parseLong(ts));
    } catch (Exception e) {
      return f.lastModified();
    }
  }
}
