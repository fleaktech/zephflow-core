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

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.HashMap;
import java.util.Map;

/** Created by bolei on 2/24/25 */
public class CefExtractionRule implements ExtractionRule {
  public static final String KEY_VERSION = "version";
  public static final String KEY_DEVICE_VENDOR = "deviceVendor";
  public static final String KEY_DEVICE_PRODUCT = "deviceProduct";
  public static final String KEY_DEVICE_VERSION = "deviceVersion";
  public static final String KEY_DEVICE_EVENT_CLASS_ID = "deviceEventClassId";
  public static final String KEY_NAME = "name";
  public static final String KEY_SEVERITY = "severity";

  @Override
  public RecordFleakData extract(String logEntry) throws Exception {
    if (logEntry == null || logEntry.isEmpty()) {
      throw new IllegalArgumentException("Log entry cannot be null or empty");
    }

    Map<String, Object> result = new HashMap<>();

    // Check if log starts with CEF:
    if (!logEntry.startsWith("CEF:")) {
      throw new IllegalArgumentException("Invalid CEF format: log entry must start with 'CEF:'");
    }

    // Split the log into header and extension parts
    int headerEndIndex = findHeaderEnd(logEntry);
    if (headerEndIndex == -1) {
      throw new IllegalArgumentException("Invalid CEF format: could not find header end");
    }

    String headerPart = logEntry.substring(0, headerEndIndex);
    String extensionPart =
        headerEndIndex < logEntry.length() ? logEntry.substring(headerEndIndex + 1) : "";

    // Parse the header
    parseHeader(headerPart, result);

    // Parse the extensions
    parseExtensions(extensionPart, result);

    return (RecordFleakData) FleakData.wrap(result);
  }

  private int findHeaderEnd(String logEntry) {
    int pipeCount = 0;
    int index = 0;
    boolean inEscape = false;

    while (index < logEntry.length() && pipeCount < 7) {
      char c = logEntry.charAt(index);

      if (c == '\\' && !inEscape) {
        inEscape = true;
      } else if (c == '|' && !inEscape) {
        pipeCount++;
      } else {
        inEscape = false;
      }

      index++;
    }

    return pipeCount == 7 ? index - 1 : -1;
  }

  private void parseHeader(String headerPart, Map<String, Object> result) {
    // Split by pipe, handling escaped pipes
    String[] fields = splitByPipe(headerPart);

    if (fields.length < 7) {
      throw new IllegalArgumentException(
          "Invalid CEF header: expected 7 fields separated by pipes, found " + fields.length);
    }

    // Parse the version from the first field (CEF:0)
    String versionField = fields[0];
    if (!versionField.startsWith("CEF:")) {
      throw new IllegalArgumentException("Invalid CEF header: must start with 'CEF:'");
    }

    try {
      int version = Integer.parseInt(versionField.substring(4));
      result.put(KEY_VERSION, version);
    } catch (NumberFormatException e) {
      // If version isn't a valid integer, store it as a string
      result.put(KEY_VERSION, versionField.substring(4));
    }

    // Store the remaining header fields
    result.put(KEY_DEVICE_VENDOR, fields[1]);
    result.put(KEY_DEVICE_PRODUCT, fields[2]);
    result.put(KEY_DEVICE_VERSION, fields[3]);
    result.put(KEY_DEVICE_EVENT_CLASS_ID, fields[4]);
    result.put(KEY_NAME, fields[5]);

    // Parse severity field
    try {
      int severity = Integer.parseInt(fields[6]);
      result.put(KEY_SEVERITY, severity);
    } catch (NumberFormatException e) {
      // If severity isn't a valid integer, store it as a string
      result.put(KEY_SEVERITY, fields[6]);
    }
  }

  private String[] splitByPipe(String input) {
    // This is a simplified implementation that doesn't handle all escape sequences
    // A more robust implementation would use a state machine or regex with lookbehinds

    boolean inEscape = false;
    StringBuilder currentField = new StringBuilder();
    java.util.List<String> fields = new java.util.ArrayList<>();

    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);

      if (c == '\\' && !inEscape) {
        inEscape = true;
      } else if (c == '|' && !inEscape) {
        fields.add(currentField.toString());
        currentField = new StringBuilder();
      } else {
        currentField.append(c);
        inEscape = false;
      }
    }

    // Add the last field
    fields.add(currentField.toString());

    return fields.toArray(new String[0]);
  }

  private void parseExtensions(String extensionPart, Map<String, Object> result) {
    if (extensionPart.isEmpty()) {
      return;
    }

    Map<String, String> labelMap =
        new HashMap<>(); // Store label mappings (e.g., cs1Label -> portalURL)

    // Start with more advanced parsing approach
    int pos = 0;
    while (pos < extensionPart.length()) {
      // Skip leading whitespace
      while (pos < extensionPart.length() && Character.isWhitespace(extensionPart.charAt(pos))) {
        pos++;
      }

      if (pos >= extensionPart.length()) {
        break;
      }

      // Find key (until equals sign)
      int equalsPos = extensionPart.indexOf('=', pos);
      if (equalsPos == -1) {
        break; // No more equals signs, we're done
      }

      String key = extensionPart.substring(pos, equalsPos).trim();
      pos = equalsPos + 1; // Move past equals sign

      // Check if value is empty
      if (pos >= extensionPart.length()
          || Character.isWhitespace(extensionPart.charAt(pos))
          || extensionPart.charAt(pos) == '=') {
        result.put(key, ""); // Empty value
        continue;
      }

      // Handle quoted values
      if (extensionPart.charAt(pos) == '"') {
        int endQuotePos = extensionPart.indexOf('"', pos + 1);
        if (endQuotePos == -1) {
          // Unterminated quote, assume it goes to the end
          String value = extensionPart.substring(pos + 1);
          result.put(key, value);
          break;
        } else {
          String value = extensionPart.substring(pos + 1, endQuotePos);
          result.put(key, value);
          pos = endQuotePos + 1;
        }
      } else {
        // Unquoted value - goes until next whitespace followed by a key=value pattern
        int valueEndPos = findValueEnd(extensionPart, pos);
        String value = extensionPart.substring(pos, valueEndPos).trim();
        result.put(key, value);
        pos = valueEndPos;
      }

      // Handle label mappings (e.g., cs1Label=portalURL & cs1=url_value)
      if (key.endsWith("Label")) {
        String baseKey = key.substring(0, key.length() - 5); // Remove "Label"
        String labelValue = (String) result.get(key);
        labelMap.put(baseKey, labelValue);
      }
    }

    // Add labeled fields with more descriptive keys
    for (Map.Entry<String, String> entry : labelMap.entrySet()) {
      String baseKey = entry.getKey();
      String label = entry.getValue();

      if (result.containsKey(baseKey)) {
        // Add a new entry with the label as key
        result.put(label, result.get(baseKey));
        // Keep the original entry too
      }
    }
  }

  private int findValueEnd(String extensionPart, int startIndex) {
    int position = startIndex;
    while (position < extensionPart.length()) {
      // Look for a pattern of "space + word + equals sign"
      // which indicates the start of a new key-value pair
      if (Character.isWhitespace(extensionPart.charAt(position))) {
        // Peek ahead to see if what follows is a key=value pattern
        int peek = position + 1;
        while (peek < extensionPart.length()
            && !Character.isWhitespace(extensionPart.charAt(peek))
            && extensionPart.charAt(peek) != '=') {
          peek++;
        }

        if (peek < extensionPart.length() && extensionPart.charAt(peek) == '=') {
          // We found a key=value pattern, so the value ends here
          return position;
        }
      }
      position++;
    }
    return extensionPart.length();
  }
}
