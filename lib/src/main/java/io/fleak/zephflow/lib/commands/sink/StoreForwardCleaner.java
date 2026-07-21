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
package io.fleak.zephflow.lib.commands.sink;

import io.fleak.zephflow.api.JobContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * Reclaims store-and-forward buffer directories left behind by replicas that no longer run on this
 * worker (scaled down, rescheduled, or belonging to deleted jobs). Grid deletes each task's working
 * directory on exit, but store-and-forward buffers deliberately live outside it to survive
 * restarts, so nothing else ever cleans them up and they accumulate forever (FLE-2241, problem 2).
 *
 * <p>A directory is reclaimed only when all three hold, so we never delete data that could still be
 * delivered:
 *
 * <ul>
 *   <li><b>unowned</b> — its {@code sf.lock} can be acquired exclusively, i.e. no live process is
 *       using it (this is the authoritative liveness check);
 *   <li><b>stale</b> — untouched for at least {@link #DEFAULT_MIN_AGE}, so a replica that is merely
 *       mid-restart is not swept out from under itself;
 *   <li><b>drained</b> — under that lock, {@link ChronicleStoreForward#isReclaimable} confirms no
 *       records remain past the delivered watermark.
 * </ul>
 *
 * <p>A directory that is unowned and stale but still holds undelivered records is kept, not deleted
 * — recovering those records is a separate concern (FLE-2241, problem 1).
 */
@Slf4j
public final class StoreForwardCleaner {

  static final Duration DEFAULT_MIN_AGE = Duration.ofMinutes(15);

  // One sweep per base directory per process: a DAG with several store-and-forward sinks shares a
  // single worker buffer tree, so the first sink to start sweeps it and the rest are no-ops.
  private static final Set<Path> SWEPT = ConcurrentHashMap.newKeySet();

  private StoreForwardCleaner() {}

  /**
   * Kicks off a background sweep of the store-and-forward base directory for this job, at most once
   * per process. {@code ownDir} is the caller's own buffer directory and is always skipped.
   */
  public static void sweepOnce(String localStorePath, JobContext jobContext, Path ownDir) {
    sweepOnce(StoreForwardPaths.baseDir(localStorePath, jobContext), ownDir);
  }

  static void sweepOnce(Path baseDir, Path ownDir) {
    if (baseDir == null || !SWEPT.add(baseDir.toAbsolutePath().normalize())) {
      return;
    }
    Thread t =
        new Thread(() -> sweep(baseDir, ownDir, DEFAULT_MIN_AGE), "sink-store-forward-cleaner");
    t.setDaemon(true);
    t.start();
  }

  static void sweep(Path baseDir, Path ownDir, Duration minAge) {
    Path base = baseDir.toAbsolutePath().normalize();
    Path own = ownDir == null ? null : ownDir.toAbsolutePath().normalize();
    if (!Files.isDirectory(base)) {
      return;
    }
    List<Path> bufferDirs = new ArrayList<>();
    try (Stream<Path> walk = Files.walk(base)) {
      walk.filter(
              p ->
                  Files.isRegularFile(p)
                      && ChronicleStoreForward.LOCK_FILE_NAME.equals(p.getFileName().toString()))
          .map(Path::getParent)
          .forEach(bufferDirs::add);
    } catch (IOException e) {
      log.warn("store-and-forward cleaner: cannot scan {}", base, e);
      return;
    }
    for (Path dir : bufferDirs) {
      try {
        maybeReclaim(base, dir, own, minAge);
      } catch (Exception e) {
        log.warn("store-and-forward cleaner: error handling {}", dir, e);
      }
    }
  }

  private static void maybeReclaim(Path base, Path dir, Path ownDir, Duration minAge)
      throws IOException {
    if (dir.equals(ownDir) || !isStale(dir, minAge)) {
      return;
    }
    if (ChronicleStoreForward.isReclaimable(dir)) {
      deleteRecursively(dir);
      pruneEmptyParents(base, dir.getParent());
      log.info("store-and-forward cleaner: reclaimed drained buffer {}", dir);
    } else {
      log.info("store-and-forward cleaner: keeping in-use or undrained buffer {}", dir);
    }
  }

  private static boolean isStale(Path dir, Duration minAge) throws IOException {
    long cutoff = System.currentTimeMillis() - minAge.toMillis();
    long newest = Files.getLastModifiedTime(dir).toMillis();
    try (Stream<Path> files = Files.list(dir)) {
      for (Path f : (Iterable<Path>) files::iterator) {
        newest = Math.max(newest, Files.getLastModifiedTime(f).toMillis());
      }
    }
    return newest <= cutoff;
  }

  private static void deleteRecursively(Path dir) throws IOException {
    try (Stream<Path> walk = Files.walk(dir)) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (IOException e) {
                  log.warn("store-and-forward cleaner: could not delete {}", p, e);
                }
              });
    }
  }

  /** Removes now-empty node/job directories up to (but not including) the base directory. */
  private static void pruneEmptyParents(Path base, Path start) {
    for (Path p = start; p != null && !p.equals(base) && p.startsWith(base); p = p.getParent()) {
      try (Stream<Path> entries = Files.list(p)) {
        if (entries.findAny().isPresent()) {
          return;
        }
      } catch (IOException e) {
        return;
      }
      try {
        Files.deleteIfExists(p);
      } catch (IOException e) {
        return;
      }
    }
  }
}
