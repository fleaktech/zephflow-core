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
import java.util.Map;
import org.junit.jupiter.api.Test;

class GenerationMigratorTest {

  @Test
  void seedsFromPriorGenerationFilteredToOwnSlice() {
    InMemoryCheckpointStore store = new InMemoryCheckpointStore();
    Instant t1 = Instant.parse("2026-01-01T00:00:00Z");
    Instant t2 = Instant.parse("2026-01-02T00:00:00Z");
    store.save("src/3/0.json", new FsCheckpoint(1, t1, Map.of("urn-0", t1, "urn-3", t1)));
    store.save("src/3/1.json", new FsCheckpoint(1, t1, Map.of("urn-1", t1, "urn-4", t1)));
    store.save("src/3/2.json", new FsCheckpoint(1, t2, Map.of("urn-2", t2, "urn-5", t2)));

    FsCheckpoint seeded = GenerationMigrator.maybeSeed(store, "src", 2, 0);
    assertNotNull(seeded);
    assertEquals(Instant.parse("2026-01-01T00:00:00Z"), seeded.watermark());
    for (String urn : seeded.completedSinceWatermark().keySet()) {
      assertEquals(0, io.fleak.zephflow.lib.commands.fssource.util.Partitioner.assignedJob(urn, 2));
    }
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
    FsCheckpoint existing =
        new FsCheckpoint(
            1,
            Instant.parse("2026-02-01T00:00:00Z"),
            Map.of("x", Instant.parse("2026-02-01T00:00:00Z")));
    store.save("src/2/0.json", existing);
    FsCheckpoint result = GenerationMigrator.maybeSeed(store, "src", 2, 0);
    assertEquals(existing, result);
  }
}
