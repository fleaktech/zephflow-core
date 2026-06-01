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
package io.fleak.zephflow.lib.commands.fssource.api;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class SizeStableProbeTest {

  private static FileLister listerWith(Map<FileKey, FileEntry> table) {
    return new FileLister() {
      @Override
      public Stream<FileEntry> list(ListRequest r) {
        return table.values().stream();
      }

      @Override
      public FileEntry stat(FileKey k) {
        return table.get(k);
      }
    };
  }

  @Test
  void firstSightingNotStable() {
    SizeStableProbe probe = new SizeStableProbe(Duration.ZERO);
    FileEntry e = new FileEntry(new FileKey("file", "u"), 10, Instant.EPOCH, "u");
    Map<FileKey, FileEntry> tbl = new HashMap<>();
    tbl.put(e.key(), e);
    assertFalse(probe.isStable(e, listerWith(tbl)));
  }

  @Test
  void secondProbeWithUnchangedSizeIsStable() {
    SizeStableProbe probe = new SizeStableProbe(Duration.ZERO);
    FileKey k = new FileKey("file", "u");
    FileEntry e = new FileEntry(k, 10, Instant.EPOCH, "u");
    Map<FileKey, FileEntry> tbl = new HashMap<>();
    tbl.put(k, e);
    assertFalse(probe.isStable(e, listerWith(tbl)));
    assertTrue(probe.isStable(e, listerWith(tbl)));
  }

  @Test
  void sizeChangedIsNotStable() {
    SizeStableProbe probe = new SizeStableProbe(Duration.ZERO);
    FileKey k = new FileKey("file", "u");
    FileEntry first = new FileEntry(k, 10, Instant.EPOCH, "u");
    FileEntry grown = new FileEntry(k, 20, Instant.EPOCH, "u");
    Map<FileKey, FileEntry> tbl = new HashMap<>();
    tbl.put(k, first);
    assertFalse(probe.isStable(first, listerWith(tbl)));
    tbl.put(k, grown);
    assertFalse(probe.isStable(grown, listerWith(tbl)));
  }
}
