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

import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;

class GenerationMigratorTest {

  @Test
  void seedsFromPriorGenerationFilteredToOwnSlice() {
    InMemoryCheckpointStore store = new InMemoryCheckpointStore();
    // Old N=3 shards: completed = {urn-0, urn-1, urn-2, urn-3, urn-4, urn-5}.
    // (Distribution among 3 shards doesn't matter for migration — we union them.)
    store.save(
        "src/3/0.json",
        new FsCheckpoint(1, Instant.parse("2026-01-01T00:00:00Z"), Set.of("urn-0", "urn-3")));
    store.save(
        "src/3/1.json",
        new FsCheckpoint(1, Instant.parse("2026-01-01T00:00:00Z"), Set.of("urn-1", "urn-4")));
    store.save(
        "src/3/2.json",
        new FsCheckpoint(1, Instant.parse("2026-01-02T00:00:00Z"), Set.of("urn-2", "urn-5")));

    // Migrate to N=2, jobIndex=0.
    FsCheckpoint seeded = GenerationMigrator.maybeSeed(store, "src", 2, 0);
    assertNotNull(seeded);
    // min watermark across shards:
    assertEquals(Instant.parse("2026-01-01T00:00:00Z"), seeded.watermark());
    // completed set: only entries where Partitioner.assignedJob(urn, 2) == 0
    for (String urn : seeded.completedSinceWatermark()) {
      assertEquals(0, io.fleak.zephflow.lib.commands.fssource.util.Partitioner.assignedJob(urn, 2));
    }
    // Seeded key persisted.
    assertTrue(store.load("src/2/0.json").isPresent());
  }

  @Test
  void returnsNullWhenNoPriorGenerations() {
    InMemoryCheckpointStore store = new InMemoryCheckpointStore();
    assertNull(GenerationMigrator.maybeSeed(store, "src", 2, 0));
  }

  @Test
  void noopIfSameGenerationAlreadyExists() {
    InMemoryCheckpointStore store = new InMemoryCheckpointStore();
    FsCheckpoint existing = new FsCheckpoint(1, Instant.parse("2026-02-01T00:00:00Z"), Set.of("x"));
    store.save("src/2/0.json", existing);
    FsCheckpoint result = GenerationMigrator.maybeSeed(store, "src", 2, 0);
    assertEquals(existing, result);
  }
}
