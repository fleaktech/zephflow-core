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
   * Extracts key-value pairs from the given raw log string using a single-pass O(n) state machine.
   *
   * @param raw The log string to parse.
   * @return A Map containing the parsed key-value pairs.
   */
  @Override
  public RecordFleakData extract(String raw) {
    Map<String, Object> resultMap = new HashMap<>();
    if (StringUtils.isBlank(raw)) {
      return new RecordFleakData();
    }

    int len = raw.length();
    int keyStart = 0;
    int lastKvSepPos = -1;
    int lastPairSepInValue = -1;
    boolean inQuotes = false;

    for (int i = 0; i < len; ) {
      char c = raw.charAt(i);
      char prevChar = (i > 0) ? raw.charAt(i - 1) : '\0';

      if (c == quoteChar && prevChar != escapeChar) {
        inQuotes = !inQuotes;
        i++;
        continue;
      }

      if (inQuotes) {
        i++;
        continue;
      }

      if (matchesSeparator(raw, i, kvSeparator)) {
        if (lastKvSepPos < 0) {
          lastKvSepPos = i;
        } else if (lastPairSepInValue >= 0) {
          emitPair(resultMap, raw, keyStart, lastKvSepPos, lastPairSepInValue);
          keyStart = lastPairSepInValue + pairSeparator.length();
          lastKvSepPos = i;
          lastPairSepInValue = -1;
        }
        i += kvSeparator.length();
        continue;
      }

      if (matchesSeparator(raw, i, pairSeparator)) {
        if (lastKvSepPos >= 0) {
          lastPairSepInValue = i;
        }
        i += pairSeparator.length();
        continue;
      }

      i++;
    }

    if (lastKvSepPos >= 0) {
      emitPair(resultMap, raw, keyStart, lastKvSepPos, len);
    }

    return (RecordFleakData) FleakData.wrap(resultMap);
  }

  private boolean matchesSeparator(String str, int pos, String separator) {
    return str.regionMatches(pos, separator, 0, separator.length());
  }

  @SuppressWarnings("unchecked")
  private void emitPair(
      Map<String, Object> resultMap, String raw, int keyStart, int kvSepPos, int valueEnd) {
    String key = raw.substring(keyStart, kvSepPos).trim();
    int valueStart = kvSepPos + kvSeparator.length();
    String value = raw.substring(valueStart, valueEnd).trim();

    if (key.isEmpty()) {
      return;
    }

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
