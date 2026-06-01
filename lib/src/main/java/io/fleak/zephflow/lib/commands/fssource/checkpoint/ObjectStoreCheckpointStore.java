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
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import io.fleak.zephflow.lib.commands.fssource.api.*;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3Backend;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendConfig;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Persists FsCheckpoint blobs as JSON via an FsBackend. Each shard has a single writer (the owning
 * job) so single-PUT atomicity at the object level is sufficient.
 *
 * <p>For local FS, writes go via tmp-file + ATOMIC_MOVE. For S3, a plain PUT is atomic at the
 * object level.
 */
public final class ObjectStoreCheckpointStore implements CheckpointStore {

  private static final Pattern GEN_SHARD = Pattern.compile("([0-9]+)/([0-9]+)\\.json$");

  private final FsBackend backend;
  private final FsBackendConfig cfg;
  private final String prefix; // e.g. "file:///tmp/_cp/" or "s3://bkt/_cp/"
  private final FileLister lister;
  private final FileReader reader;

  public ObjectStoreCheckpointStore(FsBackend backend, FsBackendConfig cfg, String prefix) {
    this.backend = backend;
    this.cfg = cfg;
    this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
    this.lister = backend.createLister(cfg);
    this.reader = backend.createReader(cfg);
  }

  @Override
  public Optional<FsCheckpoint> load(String key) {
    FileKey fk = new FileKey(backend.scheme(), prefix + key);
    try (InputStream in = reader.open(fk, 0)) {
      return Optional.of(JsonUtils.OBJECT_MAPPER.readValue(in.readAllBytes(), FsCheckpoint.class));
    } catch (UncheckedIOException | IOException e) {
      return Optional.empty();
    }
  }

  @Override
  public void save(String key, FsCheckpoint cp) {
    try {
      byte[] bytes = JsonUtils.OBJECT_MAPPER.writeValueAsBytes(cp);
      writeBytes(prefix + key, bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void writeBytes(String urn, byte[] bytes) throws IOException {
    if ("s3".equals(backend.scheme())) {
      S3BackendConfig sc = (S3BackendConfig) cfg;
      String stripped = urn.substring("s3://".length());
      int slash = stripped.indexOf('/');
      String bucket = stripped.substring(0, slash);
      String key = stripped.substring(slash + 1);
      try (S3Client c = S3Backend.client(sc)) {
        c.putObject(
            PutObjectRequest.builder().bucket(bucket).key(key).build(),
            RequestBody.fromBytes(bytes));
      }
      return;
    }
    if (!"file".equals(backend.scheme())) {
      throw new UnsupportedOperationException(
          "ObjectStoreCheckpointStore.writeBytes for " + backend.scheme() + " is wired in Phase 5");
    }
    Path target = Paths.get(java.net.URI.create(urn));
    Files.createDirectories(target.getParent());
    Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
    Files.write(tmp, bytes);
    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
  }

  @Override
  public List<Integer> listGenerations(String sourceId) {
    String root = prefix + sourceId + "/";
    Set<Integer> gens = new HashSet<>();
    try (Stream<FileEntry> s = lister.list(new ListRequest(root, null))) {
      s.forEach(
          f -> {
            var m = GEN_SHARD.matcher(f.key().urn());
            if (m.find()) gens.add(Integer.parseInt(m.group(1)));
          });
    } catch (UncheckedIOException ignored) {
      return List.of();
    }
    return new ArrayList<>(gens);
  }

  @Override
  public List<String> listShards(String sourceId, int generation) {
    String root = prefix + sourceId + "/" + generation + "/";
    List<String> normalized = new ArrayList<>();
    try (Stream<FileEntry> s = lister.list(new ListRequest(root, null))) {
      s.forEach(
          f -> {
            int i = f.key().urn().lastIndexOf(sourceId + "/");
            if (i >= 0) {
              // tail is "<gen>/<idx>.json"; prepend sourceId to get "<sourceId>/<gen>/<idx>.json"
              String tail = f.key().urn().substring(i + sourceId.length() + 1);
              normalized.add(sourceId + "/" + tail);
            }
          });
    } catch (UncheckedIOException ignored) {
    }
    return normalized;
  }

  @Override
  public void close() {
    try {
      lister.close();
    } catch (Exception ignored) {
    }
    try {
      reader.close();
    } catch (Exception ignored) {
    }
  }
}
