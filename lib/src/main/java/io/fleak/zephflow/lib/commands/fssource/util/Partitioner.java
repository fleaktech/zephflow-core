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

public final class Partitioner {

  private Partitioner() {}

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
