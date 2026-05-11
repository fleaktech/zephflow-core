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
package io.fleak.zephflow.lib.commands.piimask;

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.util.List;

public final class PiiPathWriter {

  private PiiPathWriter() {}

  @FunctionalInterface
  public interface StringRewriter {
    RewriteResult rewrite(String input);
  }

  public record RewriteResult(String output, int replacementCount) {}

  /**
   * Walks the record by path. If the leaf is a string, rewrites it via {@code rewriter} and
   * replaces in place. If the leaf is an array, rewrites each string element in place; non-string
   * elements are left untouched. Returns the total number of replacements made across all rewritten
   * strings. Returns 0 silently on any of: missing key, intermediate non-record value,
   * non-string/non-array leaf, null record.
   */
  public static int rewrite(RecordFleakData record, DottedPath path, StringRewriter rewriter) {
    if (record == null) {
      return 0;
    }
    List<String> segments = path.segments();
    RecordFleakData cursor = record;
    for (int i = 0; i < segments.size() - 1; i++) {
      FleakData next = cursor.getPayload().get(segments.get(i));
      if (!(next instanceof RecordFleakData nextRecord)) {
        return 0;
      }
      cursor = nextRecord;
    }
    String leafKey = segments.get(segments.size() - 1);
    FleakData leaf = cursor.getPayload().get(leafKey);
    if (leaf instanceof StringPrimitiveFleakData s) {
      RewriteResult r = rewriter.rewrite(s.getStringValue());
      s.setStringValue(r.output());
      return r.replacementCount();
    }
    if (leaf instanceof ArrayFleakData arr) {
      int total = 0;
      for (FleakData element : arr.getArrayPayload()) {
        if (element instanceof StringPrimitiveFleakData es) {
          RewriteResult r = rewriter.rewrite(es.getStringValue());
          es.setStringValue(r.output());
          total += r.replacementCount();
        }
      }
      return total;
    }
    return 0;
  }
}
