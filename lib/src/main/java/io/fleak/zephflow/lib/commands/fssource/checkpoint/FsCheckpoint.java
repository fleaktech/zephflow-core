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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public record FsCheckpoint(int version, Instant watermark, Set<String> completedSinceWatermark) {

  public FsCheckpoint {
    completedSinceWatermark = Set.copyOf(completedSinceWatermark);
  }

  public static FsCheckpoint empty() {
    return new FsCheckpoint(1, Instant.EPOCH, Collections.emptySet());
  }

  public FsCheckpoint withCompleted(String urn) {
    Set<String> next = new HashSet<>(completedSinceWatermark);
    next.add(urn);
    return new FsCheckpoint(version, watermark, next);
  }

  /**
   * Advance watermark to {@code newWatermark} and drop any completed entries strictly below it. The
   * pruning by-timestamp is performed by the caller, which holds the ts->urn mapping.
   */
  public FsCheckpoint withWatermark(Instant newWatermark, Set<String> retainedCompleted) {
    return new FsCheckpoint(version, newWatermark, retainedCompleted);
  }
}
