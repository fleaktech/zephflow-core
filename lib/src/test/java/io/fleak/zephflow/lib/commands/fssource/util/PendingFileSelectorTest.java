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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class PendingFileSelectorTest {

  private static PendingFile fileAt(int second) {
    String urn = "s3://b/evt_" + second;
    return new PendingFile(
        new FileEntry(new FileKey("s3", urn), 1, Instant.ofEpochSecond(second), urn),
        Instant.ofEpochSecond(second));
  }

  private static List<Integer> secondsOf(List<PendingFile> files) {
    return files.stream().map(file -> (int) file.timestamp().getEpochSecond()).toList();
  }

  @Test
  void anUncappedSelectorReturnsEverythingOldestFirst() {
    PendingFileSelector selector = new PendingFileSelector(10);
    selector.offer(fileAt(3));
    selector.offer(fileAt(1));
    selector.offer(fileAt(2));

    assertEquals(List.of(1, 2, 3), secondsOf(selector.oldestFirst()));
    assertFalse(selector.capped());
  }

  @Test
  void aCappedSelectorKeepsTheOldestFilesAndDropsTheRest() {
    PendingFileSelector selector = new PendingFileSelector(2);
    selector.offer(fileAt(5));
    selector.offer(fileAt(1));
    selector.offer(fileAt(9));
    selector.offer(fileAt(3));

    assertEquals(
        List.of(1, 3),
        secondsOf(selector.oldestFirst()),
        "the oldest files must win so the watermark can advance");
    assertTrue(selector.capped());
  }

  @Test
  void theSelectorNeverHoldsMoreThanTheCap() {
    PendingFileSelector selector = new PendingFileSelector(3);
    for (int second = 1; second <= 10_000; second++) {
      selector.offer(fileAt(second));
    }

    assertEquals(3, selector.oldestFirst().size(), "memory is bounded by the cap, not the listing");
    assertEquals(List.of(1, 2, 3), secondsOf(selector.oldestFirst()));
  }

  @Test
  void filesAtTheSameTimestampAreOrderedByUrn() {
    PendingFileSelector selector = new PendingFileSelector(10);
    PendingFile laterUrnFile = fileAt(1);
    PendingFile earlierUrnFile =
        new PendingFile(
            new FileEntry(
                new FileKey("s3", "s3://b/aaa"), 1, Instant.ofEpochSecond(1), "s3://b/aaa"),
            Instant.ofEpochSecond(1));
    selector.offer(laterUrnFile);
    selector.offer(earlierUrnFile);

    assertEquals(
        List.of("s3://b/aaa", "s3://b/evt_1"),
        selector.oldestFirst().stream().map(file -> file.entry().key().urn()).toList());
  }
}
