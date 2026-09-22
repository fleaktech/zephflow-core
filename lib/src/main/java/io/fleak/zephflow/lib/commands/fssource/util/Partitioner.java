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

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.IntStream;

public final class Partitioner {

  /**
   * How many virtual buckets files are hashed into. Fixed forever: it is baked into every stored
   * checkpoint id, so changing it orphans every checkpoint. Replicas are assigned buckets, not
   * files, which is what lets the replica count change without invalidating progress.
   */
  public static final int VIRTUAL_BUCKET_COUNT = 64;

  private Partitioner() {}

  /** The bucket a file belongs to. Depends only on the urn, never on the replica layout. */
  public static int virtualBucket(String urn) {
    return Math.floorMod(hash(urn), VIRTUAL_BUCKET_COUNT);
  }

  /** The buckets this replica is responsible for. Every bucket is owned by exactly one replica. */
  public static List<Integer> ownedBuckets(int replicaIndex, int replicaCount) {
    if (replicaCount <= 1) {
      return IntStream.range(0, VIRTUAL_BUCKET_COUNT).boxed().toList();
    }
    return IntStream.range(0, VIRTUAL_BUCKET_COUNT)
        .filter(bucket -> bucket % replicaCount == replicaIndex)
        .boxed()
        .toList();
  }

  private static int hash(String urn) {
    return Hashing.sha256().hashString(urn, StandardCharsets.UTF_8).asInt();
  }
}
