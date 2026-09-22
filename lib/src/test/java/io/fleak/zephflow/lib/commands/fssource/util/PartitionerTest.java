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
import org.junit.jupiter.api.Test;

class PartitionerTest {

  @Test
  void aUrnsBucketDoesNotDependOnTheReplicaLayout() {
    int bucket = Partitioner.virtualBucket("s3://bucket/a/b.json");

    assertTrue(bucket >= 0 && bucket < Partitioner.VIRTUAL_BUCKET_COUNT);
    assertEquals(bucket, Partitioner.virtualBucket("s3://bucket/a/b.json"), "the hash is stable");
  }

  @Test
  void aSingleReplicaOwnsEveryBucket() {
    assertEquals(Partitioner.VIRTUAL_BUCKET_COUNT, Partitioner.ownedBuckets(0, 1).size());
  }

  @Test
  void aZeroReplicaCountAlsoOwnsEveryBucket() {
    // The replicaCount <= 1 short-circuit covers 0 as well as 1 -- pin it explicitly.
    assertEquals(Partitioner.VIRTUAL_BUCKET_COUNT, Partitioner.ownedBuckets(0, 0).size());
  }

  @Test
  void everyBucketIsOwnedByExactlyOneReplica() {
    for (int replicaCount = 1; replicaCount <= 10; replicaCount++) {
      List<Integer> all = new ArrayList<>();
      for (int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++) {
        all.addAll(Partitioner.ownedBuckets(replicaIndex, replicaCount));
      }
      assertEquals(
          Partitioner.VIRTUAL_BUCKET_COUNT,
          all.size(),
          "no bucket may be owned twice at replicaCount=" + replicaCount);
      assertEquals(
          Partitioner.VIRTUAL_BUCKET_COUNT,
          new HashSet<>(all).size(),
          "no bucket may be unowned at replicaCount=" + replicaCount);
    }
  }

  @Test
  void rescalingMovesBucketsBetweenReplicasWithoutChangingAFilesBucket() {
    String urn = "s3://bucket/a/b.json";
    int bucket = Partitioner.virtualBucket(urn);

    assertTrue(Partitioner.ownedBuckets(bucket % 3, 3).contains(bucket));
    assertTrue(Partitioner.ownedBuckets(bucket % 7, 7).contains(bucket));
    assertEquals(
        bucket, Partitioner.virtualBucket(urn), "the file's identity is layout-independent");
  }
}
