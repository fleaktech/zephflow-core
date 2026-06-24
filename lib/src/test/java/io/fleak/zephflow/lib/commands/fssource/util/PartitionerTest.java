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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PartitionerTest {

  private static List<String> sampleUrns(int n) {
    List<String> urns = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      urns.add("s3://bucket/logs/2026/06/app-" + i + ".json");
    }
    return urns;
  }

  @Test
  void singleReplicaOwnsEverything() {
    for (String urn : sampleUrns(50)) {
      assertTrue(Partitioner.owns(urn, 0, 1));
    }
  }

  @Test
  void zeroOrNegativeCountOwnsEverything() {
    assertTrue(Partitioner.owns("s3://bucket/x.json", 0, 0));
  }

  @Test
  void deterministicForSameInput() {
    String urn = "s3://bucket/logs/app-42.json";
    assertEquals(Partitioner.owns(urn, 1, 3), Partitioner.owns(urn, 1, 3));
  }

  @Test
  void everyUrnOwnedByExactlyOneReplica_noGapsNoOverlap() {
    int count = 4;
    List<String> urns = sampleUrns(500);
    for (String urn : urns) {
      int owners = 0;
      for (int idx = 0; idx < count; idx++) {
        if (Partitioner.owns(urn, idx, count)) owners++;
      }
      assertEquals(1, owners, "urn must be owned by exactly one replica: " + urn);
    }
  }

  @Test
  void unionOfReplicaSubsetsIsFullSet() {
    int count = 3;
    List<String> urns = sampleUrns(300);
    Set<String> covered = new HashSet<>();
    for (int idx = 0; idx < count; idx++) {
      for (String urn : urns) {
        if (Partitioner.owns(urn, idx, count)) covered.add(urn);
      }
    }
    assertEquals(new HashSet<>(urns), covered);
  }

  @Test
  void distributionIsRoughlyEven() {
    int count = 4;
    int[] buckets = new int[count];
    List<String> urns = sampleUrns(4000);
    for (String urn : urns) {
      for (int idx = 0; idx < count; idx++) {
        if (Partitioner.owns(urn, idx, count)) buckets[idx]++;
      }
    }
    int expected = urns.size() / count;
    for (int b : buckets) {
      // Allow generous slack; we only guard against gross skew (e.g. all in one bucket).
      assertTrue(b > expected * 0.6 && b < expected * 1.4, "bucket skew: " + b);
    }
  }
}
