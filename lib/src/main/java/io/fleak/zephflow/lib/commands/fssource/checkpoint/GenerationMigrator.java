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

import io.fleak.zephflow.lib.commands.fssource.util.Partitioner;
import java.time.Instant;
import java.util.*;

public final class GenerationMigrator {

  private GenerationMigrator() {}

  public static FsCheckpoint maybeSeed(
      CheckpointStore store, String sourceId, int n, int jobIndex) {
    String currentKey = sourceId + "/" + n + "/" + jobIndex + ".json";
    Optional<FsCheckpoint> existing = store.load(currentKey);
    if (existing.isPresent()) return existing.get();

    List<Integer> prior =
        store.listGenerations(sourceId).stream()
            .filter(g -> g != n)
            .sorted(Comparator.reverseOrder())
            .toList();

    for (int prevN : prior) {
      List<String> shardKeys = store.listShards(sourceId, prevN);
      if (shardKeys.isEmpty()) continue;
      Instant minWatermark = Instant.MAX;
      Map<String, Instant> mergedCompleted = new HashMap<>();
      for (String sk : shardKeys) {
        Optional<FsCheckpoint> cp = store.load(sk);
        if (cp.isEmpty()) continue;
        if (cp.get().watermark().isBefore(minWatermark)) minWatermark = cp.get().watermark();
        mergedCompleted.putAll(cp.get().completedSinceWatermark());
      }
      if (minWatermark.equals(Instant.MAX)) minWatermark = Instant.EPOCH;
      Map<String, Instant> sliceCompleted = new HashMap<>();
      mergedCompleted.forEach(
          (urn, ts) -> {
            if (Partitioner.assignedJob(urn, n) == jobIndex) sliceCompleted.put(urn, ts);
          });
      FsCheckpoint seeded = new FsCheckpoint(1, minWatermark, sliceCompleted);
      store.save(currentKey, seeded);
      return seeded;
    }
    return null;
  }
}
