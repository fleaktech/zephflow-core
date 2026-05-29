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

class InMemoryCheckpointStoreTest {

  @Test
  void loadEmptyWhenAbsent() {
    InMemoryCheckpointStore s = new InMemoryCheckpointStore();
    assertTrue(s.load("abc/3/0.json").isEmpty());
  }

  @Test
  void saveThenLoad() {
    InMemoryCheckpointStore s = new InMemoryCheckpointStore();
    FsCheckpoint cp = new FsCheckpoint(1, Instant.parse("2026-01-01T00:00:00Z"), Set.of("u1"));
    s.save("abc/3/0.json", cp);
    assertEquals(cp, s.load("abc/3/0.json").orElseThrow());
  }

  @Test
  void listGenerationsAndShards() {
    InMemoryCheckpointStore s = new InMemoryCheckpointStore();
    s.save("abc/3/0.json", FsCheckpoint.empty());
    s.save("abc/3/1.json", FsCheckpoint.empty());
    s.save("abc/3/2.json", FsCheckpoint.empty());
    s.save("abc/5/4.json", FsCheckpoint.empty());

    assertEquals(java.util.List.of(3, 5), s.listGenerations("abc").stream().sorted().toList());
    assertEquals(3, s.listShards("abc", 3).size());
    assertEquals(1, s.listShards("abc", 5).size());
  }
}
