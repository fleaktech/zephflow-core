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

/**
 * Stateless, deterministic file-to-replica assignment. A file is owned by exactly one replica,
 * decided by a stable hash of its URN. No coordination between replicas.
 */
public final class Partitioner {

  private Partitioner() {}

  /**
   * @param urn the file's stable, scheme-prefixed URN (e.g. {@code s3://bucket/key})
   * @param replicaIndex this replica's 0-based ordinal
   * @param replicaCount total number of replicas
   * @return true if this replica owns the file; always true when {@code replicaCount <= 1}
   */
  public static boolean owns(String urn, int replicaIndex, int replicaCount) {
    if (replicaCount <= 1) {
      return true;
    }
    int bucket = Math.floorMod(hash(urn), replicaCount);
    return bucket == replicaIndex;
  }

  private static int hash(String urn) {
    return Hashing.sha256().hashString(urn, StandardCharsets.UTF_8).asInt();
  }
}
