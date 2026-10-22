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
package io.fleak.zephflow.lib.commands.filedecode;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import java.util.List;
import org.junit.jupiter.api.Test;

class RecordRangesTest {

  @Test
  void adjacentRecordsMergeWithTheBytesTheySpan() {
    RecordRanges ranges = new RecordRanges();
    ranges.add(1, new RawSpan(0, 10));
    ranges.add(2, new RawSpan(10, 5));
    ranges.add(5, 7, new RawSpan(40, 20));
    ranges.add(9, null);

    assertEquals("recordIndex 1-2 (bytes 0-15), 5-7 (bytes 40-60), 9", ranges.describe());
  }

  @Test
  void theHeaderAndTheDocumentAreNamed() {
    RecordRanges header = new RecordRanges();
    header.add(0, new RawSpan(0, 3));
    header.add(2, null);
    RecordRanges document = new RecordRanges();
    document.add(-1, new RawSpan(0, 12));

    assertEquals(
        List.of("the header (bytes 0-3), recordIndex 2", "the document (bytes 0-12)"),
        List.of(header.describe(), document.describe()));
  }

  @Test
  void rangesAfterTheCapAreOnlyCounted() {
    RecordRanges ranges = new RecordRanges();
    for (long i = 1; i <= 2 * RecordRanges.MAX_LISTED_RANGES + 4; i += 2) {
      ranges.add(i, null);
    }

    assertTrue(ranges.describe().endsWith(", 199, and 2 more ranges"));
    assertFalse(new RecordRanges().describe().contains("recordIndex"));
  }
}
