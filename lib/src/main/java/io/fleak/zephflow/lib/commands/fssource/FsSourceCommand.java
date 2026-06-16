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
import io.fleak.zephflow.lib.commands.fssource.api.*;
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendConfig;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.*;
import io.fleak.zephflow.lib.commands.fssource.emission.*;
import io.fleak.zephflow.lib.commands.fssource.util.Partitioner;
import io.fleak.zephflow.lib.commands.fssource.util.SourceIdHasher;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class FsSourceCommand extends SourceCommand {

  private volatile boolean terminated = false;
  private FsCheckpoint checkpoint = FsCheckpoint.empty();
  private String checkpointKey;

  public FsSourceCommand(String nodeId, JobContext jobContext) {
    super(nodeId, jobContext, new FsSourceConfigParser(), new FsSourceConfigValidator());
  }

  @Override
  public String commandName() {
    return "fssource";
  }

  @Override
  public SourceType sourceType() {
    FsSourceDto.Config c = (FsSourceDto.Config) commandConfig;
    return c != null && c.getMode() == FsSourceDto.Mode.UNBOUNDED
        ? SourceType.STREAMING
        : SourceType.BATCH;
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
    ec.checkpointStore = buildCheckpointStore(c, ec.backend, bc, jc);
    return ec;
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

  private static CheckpointStore buildCheckpointStore(
      FsSourceDto.Config c,
      FsBackend sourceBackend,
      FsBackendConfig sourceBackendCfg,
      JobContext jobContext) {
    FsBackend cpBackend;
    FsBackendConfig cpCfg;
    String prefixRoot;
    if (c.getCheckpoint() != null) {
      cpBackend = FsBackendRegistry.get(c.getCheckpoint().getBackend());
      cpCfg =
          buildBackendConfigForCheckpoint(
              c.getCheckpoint().getBackend(),
              c.getCheckpoint().getRoot(),
              c.getBackendConfig(),
              jobContext);
      prefixRoot = c.getCheckpoint().getRoot();
    } else {
      cpBackend = sourceBackend;
      cpCfg = sourceBackendCfg;
      prefixRoot = c.getRoot();
    }
    String prefix =
        (prefixRoot.endsWith("/") ? prefixRoot : prefixRoot + "/") + "_zephflow_checkpoints/";
    return new ObjectStoreCheckpointStore(cpBackend, cpCfg, prefix);
  }

  private static FsBackendConfig buildBackendConfigForCheckpoint(
      String backend,
      String root,
      java.util.Map<String, Object> sourceBackendConfig,
      JobContext jobContext) {
    return switch (backend) {
      case "file" -> new LocalFsBackendConfig(root);
      case "s3" -> s3BackendConfig(sourceBackendConfig);
      case "gs" -> gcsBackendConfig(sourceBackendConfig);
      case "azblob" -> azureBackendConfig(sourceBackendConfig, jobContext);
      default -> throw new IllegalArgumentException("Unsupported checkpoint backend: " + backend);
    };
  }

  @Override
  public void execute(String user, SourceEventAcceptor out) throws Exception {
    FsSourceExecutionContext ec = (FsSourceExecutionContext) getExecutionContext();
    FsSourceDto.Config c = (FsSourceDto.Config) commandConfig;

    int parallelism = resolveParallelism(c);
    int jobIndex = resolveJobIndex(c);
    String sourceId = SourceIdHasher.compute(c.getBackend(), c.getRoot(), c.getFileNameRegex());
    checkpointKey = sourceId + "/" + parallelism + "/" + jobIndex + ".json";
    FsCheckpoint seeded =
        GenerationMigrator.maybeSeed(ec.checkpointStore, sourceId, parallelism, jobIndex);
    checkpoint = seeded != null ? seeded : FsCheckpoint.empty();
    log.info(
        "fs_source open: sourceId={} key={} watermark={}",
        sourceId,
        checkpointKey,
        checkpoint.watermark());

    Pattern regex = c.getFileNameRegex() == null ? null : Pattern.compile(c.getFileNameRegex());
    EmissionStrategy emission = buildEmission(c.getEmission());
    StabilityProbe probe =
        c.getStability().isEnabled()
            ? new SizeStableProbe(Duration.ofMillis(c.getStability().getProbeDelayMs()))
            : StabilityProbe.ALWAYS_STABLE;
    PostAction postAction = buildPostAction(c.getPostAction());

    boolean bounded = c.getMode() == FsSourceDto.Mode.BOUNDED;
    long backoffMs = 100;
    long backoffCapMs = Math.max(100, c.getListingIntervalMs());

    while (!terminated) {
      ListRequest req = new ListRequest(c.getRoot(), regex);
      List<Pending> todo = new ArrayList<>();
      try (var stream = ec.lister.list(req)) {
        stream
            .filter(f -> Partitioner.assignedJob(f.key().urn(), parallelism) == jobIndex)
            .map(f -> new Pending(f, tsFromName(f, regex)))
            .filter(p -> p.ts().compareTo(checkpoint.watermark()) >= 0)
            .filter(p -> !checkpoint.isCompleted(p.entry().key().urn()))
            .sorted(Comparator.comparing(Pending::ts).thenComparing(p -> p.entry().key().urn()))
            .forEach(todo::add);
      }

      int emittedThisPass = 0;
      for (Pending p : todo) {
        FileEntry f = p.entry();
        if (!probe.isStable(f, ec.lister)) continue;
        emission.emit(f, ec.reader, out, jobContext);
        checkpoint = checkpoint.withEmitted(f.key().urn(), p.ts());
        ec.checkpointStore.save(checkpointKey, checkpoint);
        postAction.run(f, ec.backend, ec.backendConfig);
        emittedThisPass++;
      }

      if (bounded) {
        if (emittedThisPass == 0 && todo.isEmpty()) {
          out.terminate();
          return;
        }
        Thread.sleep(c.getListingIntervalMs());
        continue;
      }
      if (emittedThisPass == 0) {
        Thread.sleep(Math.min(backoffMs, backoffCapMs));
        backoffMs = Math.min(backoffMs * 2, backoffCapMs);
      } else {
        backoffMs = 100;
        Thread.sleep(c.getListingIntervalMs());
      }
    }
    out.terminate();
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

  private static EmissionStrategy buildEmission(FsSourceDto.Emission e) {
    return switch (e.getType()) {
      case LINE -> new LineEmissionStrategy(Charset.forName(e.getEncoding()), e.getLineBatchSize());
      case WHOLE_FILE -> new WholeFileEmissionStrategy(Charset.forName(e.getEncoding()));
      case FILE_REFERENCE -> new FileReferenceEmissionStrategy();
    };
  }

  private static PostAction buildPostAction(FsSourceDto.PostActionConfig pa) {
    if (pa == null || pa.getType() == FsSourceDto.PostActionType.NONE) return PostAction.NO_OP;
    return switch (pa.getType()) {
      case DELETE -> PostActions.delete();
      case ARCHIVE -> PostActions.moveTo(pa.getDestinationPrefix());
      default -> PostAction.NO_OP;
    };
  }

  private int resolveParallelism(FsSourceDto.Config c) {
    if (c.getPartition() != null) return c.getPartition().getParallelism();
    Object v = jobContext.getOtherProperties().get("zephflow.job.parallelism");
    return v instanceof Number n ? n.intValue() : 1;
  }

  private int resolveJobIndex(FsSourceDto.Config c) {
    if (c.getPartition() != null) return c.getPartition().getIndex();
    Object v = jobContext.getOtherProperties().get("zephflow.job.index");
    return v instanceof Number n ? n.intValue() : 0;
  }
}
