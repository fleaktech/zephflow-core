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

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Selects the oldest {@code maxFiles} candidates from a listing without materializing the listing.
 *
 * <p>A prefix can hold millions of objects; collecting them all and sorting is unbounded memory for
 * a run that will only ever process a prefix of the result. The heap is ordered newest-first so the
 * head is the worst candidate held, which is the one evicted when the cap is exceeded — what
 * survives is the oldest N. Oldest-first is what the watermark needs: it only advances over files
 * that completed, so whatever this drops is picked up by the next run.
 */
public final class PendingFileSelector {

  private static final Comparator<PendingFile> OLDEST_FIRST =
      Comparator.comparing(PendingFile::timestamp)
          .thenComparing(pendingFile -> pendingFile.entry().key().urn());

  private final int maxFiles;
  private final PriorityQueue<PendingFile> newestFirst;
  private boolean capped;

  public PendingFileSelector(int maxFiles) {
    if (maxFiles <= 0) {
      throw new IllegalArgumentException("maxFiles must be greater than 0, got " + maxFiles);
    }
    this.maxFiles = maxFiles;
    this.newestFirst = new PriorityQueue<>(OLDEST_FIRST.reversed());
  }

  public void offer(PendingFile candidate) {
    newestFirst.offer(candidate);
    if (newestFirst.size() > maxFiles) {
      newestFirst.poll();
      capped = true;
    }
  }

  /** The selected files, oldest first. */
  public List<PendingFile> oldestFirst() {
    return newestFirst.stream().sorted(OLDEST_FIRST).toList();
  }

  /** Whether candidates were dropped, so this run covers only part of the listing. */
  public boolean capped() {
    return capped;
  }
}
