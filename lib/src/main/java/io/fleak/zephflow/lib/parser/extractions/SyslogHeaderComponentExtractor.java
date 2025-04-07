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
package io.fleak.zephflow.lib.parser.extractions;

import static io.fleak.zephflow.lib.parser.extractions.SyslogExtractionRule.*;

import com.google.common.base.Preconditions;
import java.text.ParsePosition;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Created by bolei on 2/25/25 */
public abstract class SyslogHeaderComponentExtractor {
  protected final String fieldName;

  protected SyslogHeaderComponentExtractor(String fieldName) {
    this.fieldName = fieldName;
  }

  int extractComponent(
      Map<String, Object> result,
      String logEntry,
      int startPosition,
      boolean lastComponent,
      Character messageBodyDelimiter,
      String timestampPattern) {
    int pos = calculateComponentEnd(logEntry, startPosition);
    Preconditions.checkArgument(pos > startPosition);
    if (lastComponent
        && messageBodyDelimiter != null
        && logEntry.charAt(pos - 1) == messageBodyDelimiter) {
      pos--;
      Preconditions.checkArgument(pos > startPosition);
    }
    validateComponent(logEntry, startPosition, pos);

    Object parsedComponent = parseComponent(logEntry, startPosition, pos);
    result.put(fieldName, parsedComponent);
    pos = skipWhitespace(logEntry, pos);
    return pos;
  }

  Object parseComponent(String logEntry, int start, int end) {
    return logEntry.substring(start, end);
  }

  // end position is exclusive
  abstract int calculateComponentEnd(String logEntry, int startPosition);

  void validateComponent(String logEntry, int startPosition, int pos) {
    // no-op
  }

  static int skipWhitespace(String str, int startIndex) {
    int position = startIndex;
    while (position < str.length() && Character.isWhitespace(str.charAt(position))) {
      position++;
    }
    return position;
  }

  static int skipNonWhitespace(String str, int startIndex) {
    int position = startIndex;
    // Find the end of the token (next whitespace)
    while (position < str.length() && !Character.isWhitespace(str.charAt(position))) {
      position++;
    }
    return position;
  }

  static class PriorityComponentExtractor extends SyslogHeaderComponentExtractor {

    protected PriorityComponentExtractor() {
      super(PRIORITY_KEY);
    }

    @Override
    void validateComponent(String logEntry, int startPosition, int pos) {
      Preconditions.checkArgument(logEntry.charAt(startPosition) == '<');
      Preconditions.checkArgument(logEntry.charAt(pos - 1) == '>');
    }

    @Override
    public int extractComponent(
        Map<String, Object> result,
        String logEntry,
        int startPosition,
        boolean lastComponent,
        Character messageBodyDelimiter,
        String timestampPattern) {
      Preconditions.checkArgument(startPosition == 0);
      Preconditions.checkArgument(logEntry.charAt(0) == '<');
      int pos = logEntry.indexOf('>');
      Preconditions.checkArgument(pos > 0);
      int priority = Integer.parseInt(logEntry.substring(1, pos));
      result.put(fieldName, priority);
      pos = skipWhitespace(logEntry, pos + 1);
      return pos;
    }

    @Override
    int calculateComponentEnd(String logEntry, int startPosition) {
      return logEntry.indexOf('>') + 1;
    }
  }

  static class TimestampComponentExtractor extends SyslogHeaderComponentExtractor {

    private int endPos = -1; // Initialize with invalid position

    protected TimestampComponentExtractor() {
      super(TIMESTAMP_KEY);
    }

    @Override
    public int extractComponent(
        Map<String, Object> result,
        String logEntry,
        int startPosition,
        boolean lastComponent,
        Character messageBodyDelimiter,
        String timestampPattern) {

      // Validate input parameters
      if (logEntry == null || logEntry.isEmpty()) {
        throw new IllegalArgumentException("Log entry cannot be null or empty");
      }
      if (startPosition < 0 || startPosition >= logEntry.length()) {
        throw new IllegalArgumentException("Start position is invalid: " + startPosition);
      }
      if (timestampPattern == null || timestampPattern.isEmpty()) {
        throw new IllegalArgumentException("Timestamp pattern cannot be null or empty");
      }

      // Create a DateTimeFormatter with the given pattern
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timestampPattern, Locale.US);

      // Use ParsePosition to determine how much of the string was consumed
      ParsePosition pos = new ParsePosition(startPosition);

      try {
        formatter.parse(logEntry, pos);

        if (pos.getIndex() > startPosition) {
          // We found a timestamp, extract it and return the end position
          String timestamp = logEntry.substring(startPosition, pos.getIndex());
          endPos = pos.getIndex();
          result.put(fieldName, timestamp); // Use inherited componentKey
          return skipWhitespace(logEntry, endPos);
        }
      } catch (Exception e) {
        // If an exception occurs during parsing, no timestamp was found
      }

      // If we get here, no timestamp was found
      throw new IllegalArgumentException(
          "No timestamp matching the pattern '"
              + timestampPattern
              + "' found in the log entry starting at position "
              + startPosition);
    }

    @Override
    int calculateComponentEnd(String logEntry, int startPosition) {
      return endPos;
    }
  }

  static class NoWhitespaceComponentExtractor extends SyslogHeaderComponentExtractor {

    protected NoWhitespaceComponentExtractor(String fieldName) {
      super(fieldName);
    }

    @Override
    int calculateComponentEnd(String logEntry, int startPosition) {
      return skipNonWhitespace(logEntry, startPosition);
    }
  }

  static class StructuredDataComponentExtractor extends SyslogHeaderComponentExtractor {

    protected StructuredDataComponentExtractor() {
      super(STRUCTURED_DATA_KEY);
    }

    @Override
    int calculateComponentEnd(String logEntry, int startPosition) {
      // Check if we have a valid starting position
      if (startPosition >= logEntry.length()) {
        throw new IllegalArgumentException("Start position is beyond the end of the log entry");
      }

      // Check if this is a simple "-" (no structured data)
      if (logEntry.charAt(startPosition) == '-') {
        return startPosition + 1;
      }

      // Check if the component starts with a bracket
      if (logEntry.charAt(startPosition) != '[') {
        throw new IllegalArgumentException(
            "Structured data must start with '[' at position " + startPosition);
      }

      int position = startPosition;
      boolean inQuotes = false;
      boolean escapeNext = false;

      // Process multiple sequential structured data elements
      while (position < logEntry.length() && logEntry.charAt(position) == '[') {
        // Find the end of this structured data element
        position++;

        while (position < logEntry.length()) {
          char c = logEntry.charAt(position);

          if (escapeNext) {
            // Skip this character as it's escaped
            escapeNext = false;
          } else {
            if (c == '\\') {
              // Next character is escaped
              escapeNext = true;
            } else if (c == '"') {
              // Toggle quote state
              inQuotes = !inQuotes;
            } else if (c == ']' && !inQuotes) {
              // Found closing bracket (when not in quotes)
              position++;
              // Exit this structured data element
              break;
            }
          }

          position++;
        }

        // Skip whitespace between structured data elements
        position = skipWhitespace(logEntry, position);
      }

      // If we're still in quotes or escaping at the end, the input was malformed
      if (inQuotes || escapeNext) {
        throw new IllegalArgumentException(
            "Malformed structured data: unclosed quotes or incomplete escape sequence");
      }

      return position;
    }

    @Override
    Object parseComponent(String logEntry, int start, int end) {
      Map<String, Map<String, String>> result = new HashMap<>();

      // Quick validation of input
      if (start >= end || start >= logEntry.length()) {
        return result;
      }

      // Check for nil value (just a hyphen)
      if (logEntry.charAt(start) == '-' && end - start == 1) {
        return "-";
      }

      int pos = start;
      char c;

      while (pos < end) {
        // Skip whitespace
        while (pos < end && Character.isWhitespace(logEntry.charAt(pos))) {
          pos++;
        }

        if (pos >= end || logEntry.charAt(pos) != '[') {
          break;
        }

        // Parse one SD-Element
        pos++; // Skip '['

        // Extract SD-ID
        int sdIdStart = pos;
        while (pos < end) {
          c = logEntry.charAt(pos);
          if (c == ']' || Character.isWhitespace(c)) {
            break;
          }
          pos++;
        }

        if (pos >= end) {
          throw new IllegalArgumentException("Malformed structured data: unclosed SD-Element");
        }

        // Check for valid SD-ID
        if (pos == sdIdStart) {
          throw new IllegalArgumentException("Malformed structured data: empty SD-ID");
        }

        String sdId = logEntry.substring(sdIdStart, pos);
        Map<String, String> params = new HashMap<>();
        result.put(sdId, params);

        // Parse parameters
        while (pos < end) {
          // Skip whitespace
          while (pos < end && Character.isWhitespace(logEntry.charAt(pos))) {
            pos++;
          }

          if (pos >= end) {
            throw new IllegalArgumentException("Malformed structured data: unclosed SD-Element");
          }

          // Check for end of SD-Element
          if (logEntry.charAt(pos) == ']') {
            pos++; // Skip closing bracket
            break;
          }

          // Parse parameter name
          int paramNameStart = pos;
          while (pos < end) {
            c = logEntry.charAt(pos);
            if (c == '=' || Character.isWhitespace(c) || c == ']') {
              break;
            }
            pos++;
          }

          if (pos >= end || logEntry.charAt(pos) != '=') {
            throw new IllegalArgumentException(
                "Malformed structured data: invalid parameter format");
          }

          String paramName = logEntry.substring(paramNameStart, pos);
          pos++; // Skip '='

          // Parameter value must start with quote
          if (pos >= end || logEntry.charAt(pos) != '"') {
            throw new IllegalArgumentException(
                "Malformed structured data: parameter value must be quoted");
          }
          pos++; // Skip opening quote

          // Extract parameter value with escape sequence handling
          StringBuilder paramValue = new StringBuilder();
          boolean escaped = false;

          while (pos < end) {
            c = logEntry.charAt(pos);

            if (escaped) {
              // Only ", \, and ] can be escaped in RFC 5424
              if (c == '"' || c == '\\' || c == ']') {
                paramValue.append(c);
              } else {
                throw new IllegalArgumentException(
                    "Malformed structured data: invalid escape sequence '\\" + c + "'");
              }
              escaped = false;
            } else if (c == '\\') {
              escaped = true;
            } else if (c == '"') {
              pos++; // Skip closing quote
              break;
            } else {
              paramValue.append(c);
            }

            pos++;
          }

          // Ensure we didn't run past the end or have unclosed quotes
          if (pos > end || (pos == end && logEntry.charAt(pos - 1) != '"')) {
            throw new IllegalArgumentException(
                "Malformed structured data: unclosed parameter value");
          }

          params.put(paramName, paramValue.toString());
        }
      }

      return result;
    }
  }
}
