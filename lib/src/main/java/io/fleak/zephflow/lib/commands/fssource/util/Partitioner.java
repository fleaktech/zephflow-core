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

  public static int assignedJob(String urn, int parallelism) {
    if (parallelism <= 0) {
      throw new IllegalArgumentException("parallelism must be > 0, got " + parallelism);
    }
    long h = Hashing.murmur3_128().hashString(urn, StandardCharsets.UTF_8).asLong();
    return Math.floorMod(h, parallelism);
  }
}
