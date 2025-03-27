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

import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_TS;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.parser.TimestampExtractor;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

/** Created by bolei on 2/1/25 */
public record WindowsMultilineExtractionRule(TimestampExtractor timestampExtractor)
    implements ExtractionRule {

  private static final String REGEX_KEY = "\\w[\\w\\s()]*";
  private static final String REGEX_VALUE = ".+";
  private static final String REGEX_KV_PAIR =
      String.format("(%s)[:=]\\s*(%s)", REGEX_KEY, REGEX_VALUE);

  private static final Pattern PATTERN_KV_PAIR = Pattern.compile(REGEX_KV_PAIR);

  // Patterns for different line types
  private static final Pattern PATTERN_LINE_NON_INDENTED_KV_PAIR = Pattern.compile(REGEX_KV_PAIR);
  private static final Pattern PATTERN_LINE_INDENTED_KV_PAIR =
      Pattern.compile("\\s+" + REGEX_KV_PAIR);
  private static final Pattern PATTERN_LINE_KEY_ONLY = Pattern.compile("(" + REGEX_KEY + ")[:=]");

  enum ParserState {
    INIT,
    ROOT,
    NESTED,
    GATHER_DESCRIPTION
  }

  @Override
  public RecordFleakData extract(String raw) throws Exception {
    Map<String, Object> root = new HashMap<>();
    Map<String, Object> nested = null;
    StringBuilder descBuilder = new StringBuilder();
    ParserState parserState = ParserState.INIT;
    BufferedReader reader = new BufferedReader(new StringReader(raw));
    String line;
    while ((line = reader.readLine()) != null) {
      switch (parserState) {
        case INIT -> {
          if (line.matches(PATTERN_LINE_NON_INDENTED_KV_PAIR.pattern())
              || line.matches(PATTERN_LINE_INDENTED_KV_PAIR.pattern())) {
            Pair<String, String> kv = parseKv(line);
            root.put(kv.getKey(), kv.getValue());
            parserState = ParserState.ROOT;
          } else if (line.matches(PATTERN_LINE_KEY_ONLY.pattern())) {
            String key = parseKeyName(line);
            nested = new HashMap<>();
            root.put(key, nested);
            parserState = ParserState.NESTED;
          }
        }
        case ROOT -> {
          if (line.matches(PATTERN_LINE_NON_INDENTED_KV_PAIR.pattern())
              || line.matches(PATTERN_LINE_INDENTED_KV_PAIR.pattern())) {
            Pair<String, String> kv = parseKv(line);
            root.put(kv.getKey(), kv.getValue());
          } else if (line.matches(PATTERN_LINE_KEY_ONLY.pattern())) {
            String key = parseKeyName(line);
            nested = new HashMap<>();
            root.put(key, nested);
            parserState = ParserState.NESTED;
          }
        }
        case NESTED -> {
          if (StringUtils.isBlank(line)) {
            continue;
          }
          Preconditions.checkNotNull(nested);
          if (line.matches(PATTERN_LINE_INDENTED_KV_PAIR.pattern())) {
            Pair<String, String> kv = parseKv(line);
            nested.put(kv.getKey(), kv.getValue());
          } else if (line.matches(PATTERN_LINE_NON_INDENTED_KV_PAIR.pattern())) {
            nested = null;
            Pair<String, String> kv = parseKv(line);
            root.put(kv.getKey(), kv.getValue());
            parserState = ParserState.ROOT;
          } else if (line.matches(PATTERN_LINE_KEY_ONLY.pattern())) {
            String key = parseKeyName(line);
            nested = new HashMap<>();
            root.put(key, nested);
          } else {
            descBuilder.append(line).append("\n");
            parserState = ParserState.GATHER_DESCRIPTION;
          }
        }
        case GATHER_DESCRIPTION -> {
          if (StringUtils.isBlank(line)) {
            continue;
          }
          descBuilder.append(line).append("\n");
        }
      }
    }
    root.put("description", descBuilder.toString().trim());
    root.put(FIELD_NAME_RAW, raw);
    if (timestampExtractor != null) {
      String tsStr = timestampExtractor.extractTimestampString(root);
      if (tsStr != null) {
        root.put(FIELD_NAME_TS, tsStr);
      }
    }
    return (RecordFleakData) FleakData.wrap(root);
  }

  private String parseKeyName(String line) {
    Matcher matcher = PATTERN_LINE_KEY_ONLY.matcher(line);
    Preconditions.checkArgument(matcher.matches());
    return matcher.group(1);
  }

  private Pair<String, String> parseKv(String line) {
    Matcher matcher = PATTERN_KV_PAIR.matcher(line.trim());
    boolean matches = matcher.matches();
    Preconditions.checkArgument(matches);
    return Pair.of(matcher.group(1), matcher.group(2));
  }

  public static TimestampExtractor createTimestampExtractor(
      WindowsMultilineExtractionConfig extractionConfig) {
    return switch (extractionConfig.timestampLocationType) {
      case FIRST_LINE -> new FirstLineTimestampExtractor();
      case FROM_FIELD ->
          new FromFieldTimestampExtractor(
              (String)
                  extractionConfig.config.get(FromFieldTimestampExtractor.CONFIG_TARGET_FIELD));
      case NO_TIMESTAMP -> null;
    };
  }

  static class FirstLineTimestampExtractor implements TimestampExtractor {

    @Override
    public String extractTimestampString(Map<String, Object> payload) {
      String raw = (String) payload.get(FIELD_NAME_RAW);
      if (raw == null) {
        return null;
      }
      String[] lines = raw.lines().toArray(String[]::new);
      return lines[0];
    }
  }

  static class FromFieldTimestampExtractor implements TimestampExtractor {

    static final String CONFIG_TARGET_FIELD = "target_field";

    private final String fieldName;

    FromFieldTimestampExtractor(String fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String extractTimestampString(Map<String, Object> payload) {
      Object val = payload.get(fieldName);
      if (val instanceof String) {
        return (String) val;
      }
      return null;
    }
  }
}
