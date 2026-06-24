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

  private static List<String> sampleUrns(int size) {
    List<String> urns = new ArrayList<>();
    for (int index = 0; index < size; index++) {
      urns.add("s3://bucket/logs/2026/06/app-" + index + ".json");
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
    int replicaCount = 4;
    List<String> urns = sampleUrns(500);
    for (String urn : urns) {
      int owners = 0;
      for (int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++) {
        if (Partitioner.owns(urn, replicaIndex, replicaCount)) owners++;
      }
      assertEquals(1, owners, "urn must be owned by exactly one replica: " + urn);
    }
  }

  @Test
  void unionOfReplicaSubsetsIsFullSet() {
    int replicaCount = 3;
    List<String> urns = sampleUrns(300);
    Set<String> covered = new HashSet<>();
    for (int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++) {
      for (String urn : urns) {
        if (Partitioner.owns(urn, replicaIndex, replicaCount)) covered.add(urn);
      }
    }
    assertEquals(new HashSet<>(urns), covered);
  }

  @Test
  void distributionIsRoughlyEven() {
    int replicaCount = 4;
    int[] bucketSizes = new int[replicaCount];
    List<String> urns = sampleUrns(4000);
    for (String urn : urns) {
      for (int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++) {
        if (Partitioner.owns(urn, replicaIndex, replicaCount)) bucketSizes[replicaIndex]++;
      }
    }
    int expectedPerBucket = urns.size() / replicaCount;
    for (int bucketSize : bucketSizes) {
      assertTrue(
          bucketSize > expectedPerBucket * 0.6 && bucketSize < expectedPerBucket * 1.4,
          "bucket skew: " + bucketSize);
    }
  }
}
