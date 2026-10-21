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

import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import java.util.ArrayList;
import java.util.List;

/**
 * Record index ranges, adjacent ones merged, with the bytes they span when known. Only the first
 * {@link #MAX_LISTED_RANGES} are kept; the rest are counted. Record index 0 (a csv header) is shown
 * as "the header", and -1 (a whole document) as "the document".
 */
public class RecordRanges {

  static final int MAX_LISTED_RANGES = 100;

  private final List<long[]> ranges = new ArrayList<>(); // first, last, spanStart, spanEnd
  private long lastIndex = Long.MIN_VALUE;
  private long dropped;

  public void add(long recordIndex, RawSpan span) {
    add(recordIndex, recordIndex, span);
  }

  /** Adds the records {@code first} to {@code last}; a record index under 1 is a document. */
  public void add(long first, long last, RawSpan span) {
    long start = span != null && span.length() >= 0 ? span.offset() : -1;
    long end = start >= 0 ? span.offset() + span.length() : -1;
    boolean adjacent = first >= 1 && lastIndex + 1 == first;
    lastIndex = last;
    if (adjacent && dropped == 0) {
      long[] previous = ranges.getLast();
      previous[1] = last;
      if (start >= 0) {
        previous[2] = previous[2] < 0 ? start : Math.min(previous[2], start);
        previous[3] = Math.max(previous[3], end);
      }
    } else if (adjacent) {
      return;
    } else if (ranges.size() < MAX_LISTED_RANGES) {
      ranges.add(new long[] {first, last, start, end});
    } else {
      dropped++;
    }
  }

  public boolean isEmpty() {
    return ranges.isEmpty();
  }

  /** "recordIndex 1-3 (bytes 0-99), 7 (bytes 200-250)", or "the document (bytes 0-12)". */
  public String describe() {
    StringBuilder out = new StringBuilder();
    boolean indexed = false;
    for (int i = 0; i < ranges.size(); i++) {
      long[] r = ranges.get(i);
      if (i > 0) {
        out.append(", ");
      }
      if (r[0] < 1) {
        out.append(r[0] == 0 ? "the header" : "the document");
      } else {
        if (!indexed) {
          out.append("recordIndex ");
          indexed = true;
        }
        out.append(r[0] == r[1] ? Long.toString(r[0]) : r[0] + "-" + r[1]);
      }
      if (r[2] >= 0) {
        out.append(" (bytes ").append(r[2]).append('-').append(r[3]).append(')');
      }
    }
    if (dropped > 0) {
      out.append(", and ").append(dropped).append(" more ranges");
    }
    return out.toString();
  }
}
