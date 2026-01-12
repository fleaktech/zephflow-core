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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * An extraction rule that parses a string of key-value pairs into a Map.
 *
 * <p>This class is configurable for both the pair separator (e.g., ",") and the key-value separator
 * (e.g., "="). It supports multi-character separators (e.g., "=>", ", ") and escape sequences
 * (e.g., "\t" for tab). It correctly handles values that are enclosed in double quotes, allowing
 * them to contain separators.
 */
public class KvPairsExtractionRule implements ExtractionRule {
  private final String pairSeparator;
  private final String kvSeparator;
  private final char quoteChar = '"';
  private final char escapeChar = '\\';

  /** Constructs a new extraction rule with specified separators. */
  public KvPairsExtractionRule(KvPairExtractionConfig kvPairExtractionConfig) {
    this.pairSeparator = kvPairExtractionConfig.getUnescapedPairSeparator();
    this.kvSeparator = kvPairExtractionConfig.getUnescapedKvSeparator();

    if (StringUtils.isEmpty(pairSeparator)) {
      throw new IllegalArgumentException("Pair separator cannot be empty.");
    }
    if (StringUtils.isEmpty(kvSeparator)) {
      throw new IllegalArgumentException("Key-Value separator cannot be empty.");
    }
    if (pairSeparator.equals(kvSeparator)) {
      throw new IllegalArgumentException(
          "Pair separator and Key-Value separator cannot be the same.");
    }
  }

  /**
   * Extracts key-value pairs from the given raw log string.
   *
   * @param raw The log string to parse.
   * @return A Map containing the parsed key-value pairs.
   * @throws IOException if the underlying parser fails.
   */
  @Override
  @SuppressWarnings("unchecked")
  public RecordFleakData extract(String raw) throws IOException {
    Map<String, Object> resultMap = new HashMap<>();
    if (StringUtils.isBlank(raw)) {
      return new RecordFleakData();
    }

    List<String> pairs = splitRespectingQuotes(raw, pairSeparator);

    for (String pair : pairs) {
      if (StringUtils.isBlank(pair)) {
        continue;
      }

      int separatorIndex = findSeparatorOutsideQuotes(pair, kvSeparator);

      if (separatorIndex == -1) {
        throw new IllegalArgumentException("no valid key-value separator found: " + pair);
      }

      String key = pair.substring(0, separatorIndex).trim();
      String value = pair.substring(separatorIndex + kvSeparator.length()).trim();

      String unquotedKey = unquoteAndUnescape(key);
      String unquotedValue = unquoteAndUnescape(value);

      if (resultMap.containsKey(unquotedKey)) {
        Object existing = resultMap.get(unquotedKey);
        if (existing instanceof List) {
          ((List<String>) existing).add(unquotedValue);
        } else {
          List<String> list = new ArrayList<>();
          list.add((String) existing);
          list.add(unquotedValue);
          resultMap.put(unquotedKey, list);
        }
      } else {
        resultMap.put(unquotedKey, unquotedValue);
      }
    }
    return (RecordFleakData) FleakData.wrap(resultMap);
  }

  private int findSeparatorOutsideQuotes(String str, String separator) {
    boolean inQuotes = false;
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      char prevChar = (i > 0) ? str.charAt(i - 1) : '\0';

      if (c == this.quoteChar && prevChar != this.escapeChar) {
        inQuotes = !inQuotes;
      } else if (!inQuotes && str.startsWith(separator, i)) {
        return i;
      }
    }
    return -1;
  }

  /** Splits a string by a separator, but ignores separators inside quoted sections. */
  private List<String> splitRespectingQuotes(String str, String separator) {
    List<String> parts = new ArrayList<>();
    StringBuilder currentPart = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      char prevChar = (i > 0) ? str.charAt(i - 1) : '\0';

      if (c == this.quoteChar && prevChar != this.escapeChar) {
        inQuotes = !inQuotes;
      }

      if (!inQuotes && str.startsWith(separator, i)) {
        parts.add(currentPart.toString().trim());
        currentPart.setLength(0);
        i += separator.length() - 1; // -1 because the loop will increment
      } else {
        currentPart.append(c);
      }
    }
    parts.add(currentPart.toString().trim());
    return parts;
  }

  /** Checks if a string starts and ends with a double quote. */
  private boolean isQuoted(String str) {
    return str.startsWith(String.valueOf(quoteChar)) && str.endsWith(String.valueOf(quoteChar));
  }

  /** Removes surrounding quotes and un-escapes any escaped quotes within. */
  private String unquoteAndUnescape(String str) {
    if (str == null) {
      return null;
    }
    if (!isQuoted(str)) {
      return str;
    }

    // Remove surrounding quotes
    String inner = str.substring(1, str.length() - 1);
    // Un-escape quotes
    return inner.replace(
        String.valueOf(this.escapeChar) + this.quoteChar, String.valueOf(this.quoteChar));
  }
}
