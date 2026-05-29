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
package io.fleak.zephflow.lib.commands.fssource.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PartitionerTest {

  @Test
  void deterministic() {
    int a = Partitioner.assignedJob("s3://bucket/file-001.json", 4);
    int b = Partitioner.assignedJob("s3://bucket/file-001.json", 4);
    assertEquals(a, b);
  }

  @Test
  void inRange() {
    for (int i = 0; i < 1000; i++) {
      int slot = Partitioner.assignedJob("s3://bucket/file-" + i, 8);
      assertTrue(slot >= 0 && slot < 8, "slot=" + slot);
    }
  }

  @Test
  void coversAllSlotsAndIsDisjoint() {
    int n = 4;
    Set<String> all = new HashSet<>();
    Set<String>[] slots = new Set[n];
    for (int i = 0; i < n; i++) slots[i] = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      String urn = "s3://bkt/f-" + i;
      all.add(urn);
      slots[Partitioner.assignedJob(urn, n)].add(urn);
    }
    Set<String> union = new HashSet<>();
    for (Set<String> s : slots) {
      for (String u : s) {
        assertTrue(union.add(u), "Duplicate across slots: " + u);
      }
    }
    assertEquals(all, union);
  }

  @Test
  void singleJobOwnsEverything() {
    for (int i = 0; i < 100; i++) {
      assertEquals(0, Partitioner.assignedJob("any-urn-" + i, 1));
    }
  }

  @Test
  void rejectsZeroParallelism() {
    assertThrows(IllegalArgumentException.class, () -> Partitioner.assignedJob("x", 0));
  }
}
