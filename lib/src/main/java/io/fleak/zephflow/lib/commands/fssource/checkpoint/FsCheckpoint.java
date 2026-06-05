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

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public record FsCheckpoint(
    int version, Instant watermark, Map<String, Instant> completedSinceWatermark) {

  public FsCheckpoint {
    completedSinceWatermark = Map.copyOf(completedSinceWatermark);
  }

  public static FsCheckpoint empty() {
    return new FsCheckpoint(1, Instant.EPOCH, Map.of());
  }

  public boolean isCompleted(String urn) {
    return completedSinceWatermark.containsKey(urn);
  }

  public FsCheckpoint withEmitted(String urn, Instant ts) {
    Instant newWatermark = ts.isAfter(watermark) ? ts : watermark;
    Map<String, Instant> next = new HashMap<>();
    completedSinceWatermark.forEach(
        (u, t) -> {
          if (!t.isBefore(newWatermark)) next.put(u, t);
        });
    next.put(urn, ts);
    return new FsCheckpoint(version, newWatermark, next);
  }
}
