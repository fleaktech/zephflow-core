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
package io.fleak.zephflow.lib.serdes.decode.csv;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public final class CsvRecognition {
  public static final CsvDialect DIALECT =
      new CsvDialect(',', '"', -1, -1, false, 0, false, null, StandardCharsets.UTF_8);

  private CsvRecognition() {}

  public static boolean matches(String text) {
    if (!quotedCorrectly(text)) return false;
    int columns = -1;
    try (CsvScanner scanner =
        new CsvScanner(
            new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)), DIALECT, 0, true)) {
      CsvScanner.Frame frame;
      while ((frame = scanner.next()) != null) {
        if (!(frame instanceof CsvScanner.Row row)
            || row.problem() != null
            || row.fields().size() < 2
            || (columns >= 0 && columns != row.fields().size())) {
          return false;
        }
        columns = row.fields().size();
      }
      return columns >= 2;
    } catch (IOException exception) {
      return false;
    }
  }

  private static boolean quotedCorrectly(String text) {
    boolean quoted = false;
    boolean fieldStart = true;
    boolean closed = false;
    for (int index = 0; index < text.length(); index++) {
      char character = text.charAt(index);
      if (quoted) {
        if (character == '"') {
          if (index + 1 < text.length() && text.charAt(index + 1) == '"') index++;
          else {
            quoted = false;
            closed = true;
          }
        }
      } else if (character == ',' || character == '\r' || character == '\n') {
        fieldStart = true;
        closed = false;
      } else if (character == '"') {
        if (!fieldStart || closed) return false;
        quoted = true;
        fieldStart = false;
      } else {
        if (closed) return false;
        fieldStart = false;
      }
    }
    return !quoted;
  }
}
