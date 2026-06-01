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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.fssource.api.FsBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackendConfig;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ObjectStoreCheckpointStoreTest {

  @Test
  void roundTrip(@TempDir Path tmp) throws Exception {
    FsBackend backend = new LocalFsBackend();
    LocalFsBackendConfig cfg = new LocalFsBackendConfig(tmp.toString());
    String prefix = tmp.toUri() + "_cp/";
    ObjectStoreCheckpointStore store = new ObjectStoreCheckpointStore(backend, cfg, prefix);

    FsCheckpoint cp = new FsCheckpoint(1, Instant.parse("2026-01-01T00:00:00Z"), Set.of("a", "b"));
    store.save("abc/3/0.json", cp);

    FsCheckpoint loaded = store.load("abc/3/0.json").orElseThrow();
    assertEquals(cp.watermark(), loaded.watermark());
    assertEquals(cp.completedSinceWatermark(), loaded.completedSinceWatermark());
  }

  @Test
  void listGenerationsAndShards(@TempDir Path tmp) throws Exception {
    FsBackend backend = new LocalFsBackend();
    LocalFsBackendConfig cfg = new LocalFsBackendConfig(tmp.toString());
    String prefix = tmp.toUri() + "_cp/";
    ObjectStoreCheckpointStore store = new ObjectStoreCheckpointStore(backend, cfg, prefix);

    store.save("abc/3/0.json", FsCheckpoint.empty());
    store.save("abc/3/1.json", FsCheckpoint.empty());
    store.save("abc/5/0.json", FsCheckpoint.empty());

    assertEquals(java.util.List.of(3, 5), store.listGenerations("abc").stream().sorted().toList());
    assertEquals(2, store.listShards("abc", 3).size());
  }
}
