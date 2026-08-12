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
package io.fleak.zephflow.lib.serdes.des;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Base for formats where each line of the payload is an independent record. Subclasses only parse a
 * single line; this class provides both the strict whole-payload behavior (first bad line aborts
 * the payload) and a per-line variant that lets callers keep the good lines and report the bad
 * ones.
 *
 * <p>Blank lines are ignored rather than treated as malformed records.
 */
public abstract class LineOrientedTypedDeserializer<T> extends MultipleEventsTypedDeserializer<T> {

  /** Parses a single non-blank line into zero or more typed events. */
  protected abstract List<T> deserializeLine(String line);

  @Override
  protected final List<T> deserializeToMultipleTypedEvent(byte[] value) {
    List<T> events = new ArrayList<>();
    for (Line line : splitLines(value)) {
      events.addAll(deserializeLine(line.text()));
    }
    return events;
  }

  /** Parses each line independently, capturing per-line failures instead of throwing. */
  public final List<LineOutcome<T>> deserializeEachLine(byte[] value) {
    List<LineOutcome<T>> outcomes = new ArrayList<>();
    for (Line line : splitLines(value)) {
      try {
        outcomes.add(new LineOutcome<>(line, deserializeLine(line.text()), null));
      } catch (Exception e) {
        outcomes.add(new LineOutcome<>(line, null, e));
      }
    }
    return outcomes;
  }

  static List<Line> splitLines(byte[] value) {
    String raw = new String(value, StandardCharsets.UTF_8);
    List<Line> lines = new ArrayList<>();
    int lineNumber = 0;
    for (String text : (Iterable<String>) raw.lines()::iterator) {
      lineNumber++;
      if (text.isBlank()) {
        continue;
      }
      lines.add(new Line(lineNumber, text));
    }
    return lines;
  }

  /**
   * @param number 1-based line number within the payload
   */
  public record Line(int number, String text) {}

  /** Outcome of one line: exactly one of {@code events} and {@code error} is non-null. */
  public record LineOutcome<T>(Line line, List<T> events, Exception error) {
    public boolean failed() {
      return error != null;
    }
  }
}
