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
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

/**
 * Windows multiline extraction rule for parsing structured Windows event logs. Supports nested
 * key-value pairs, description text, and XML content.
 *
 * <p>Created by bolei on 2/1/25
 */
public record WindowsMultilineExtractionRule(TimestampExtractor timestampExtractor)
    implements ExtractionRule {

  // Regex patterns for different Windows log formats
  private static final String REGEX_KEY = "[\\w\\s().-]+";
  private static final String REGEX_VALUE = ".+";
  private static final String REGEX_KV_PAIR =
      String.format("(%s)[:=]\\s*(%s)", REGEX_KEY, REGEX_VALUE);

  // Compiled patterns for performance
  private static final Pattern PATTERN_KV_PAIR = Pattern.compile(REGEX_KV_PAIR);
  private static final Pattern PATTERN_LINE_NON_INDENTED_KV_PAIR = Pattern.compile(REGEX_KV_PAIR);
  private static final Pattern PATTERN_LINE_INDENTED_KV_PAIR =
      Pattern.compile("\\s+" + REGEX_KV_PAIR);
  private static final Pattern PATTERN_LINE_KEY_ONLY = Pattern.compile("(" + REGEX_KEY + "):\\s*$");
  private static final Pattern PATTERN_WINDOWS_EVENTVIEWER_KV =
      Pattern.compile("([\\w\\s]+):\\s+(.+)");
  private static final Pattern PATTERN_MULTILINE_VALUE_CONTINUATION =
      Pattern.compile("\\s{3,}\\S.*");

  // Patterns for value type detection
  private static final Pattern PATTERN_PRIVILEGE_NAME =
      Pattern.compile("Se[A-Z][a-zA-Z]*Privilege");
  private static final Pattern PATTERN_SINGLE_WORD = Pattern.compile("[A-Z][a-zA-Z]*");
  private static final Pattern PATTERN_NUMBER = Pattern.compile("\\d+");
  private static final Pattern PATTERN_HEX = Pattern.compile("0x[0-9a-fA-F]+");
  private static final Pattern PATTERN_SID = Pattern.compile("S-\\d+-\\d+.*");
  private static final Pattern PATTERN_GUID = Pattern.compile("\\{[0-9a-fA-F-]+}");

  // Field name constants
  private static final String FIELD_DESCRIPTION = "Description";
  private static final String FIELD_DESCRIPTION_LOWER = "description";
  private static final String FIELD_EVENT_XML = "Event Xml";

  // String constants to avoid magic strings
  private static final String BACKSLASH = "\\";
  private static final String AT_SYMBOL = "@";
  private static final String PROTOCOL_SEPARATOR = "://";
  private static final String EQUALS = "=";
  private static final String COLON = ":";

  // Length thresholds
  private static final int MULTILINE_VALUE_MAX_LENGTH = 30;
  private static final int COMPLETE_VALUE_MAX_LENGTH = 50;

  /** Parser states for handling different sections of Windows event logs. */
  enum ParserState {
    INIT,
    ROOT,
    NESTED,
    GATHER_DESCRIPTION,
    GATHER_XML,
    GATHER_MULTILINE_VALUE
  }

  /** Context class to maintain parser state between line processing. */
  private static class ParserContext {
    ParserState state = ParserState.INIT;
    Map<String, Object> nested = null;
    final StringBuilder descBuilder = new StringBuilder();
    final StringBuilder xmlBuilder = new StringBuilder();
    final StringBuilder multilineValueBuilder = new StringBuilder();
    String currentDescriptionKey = null;
    String currentMultilineKey = null;
    Map<String, Object> currentMultilineTarget = null;

    void clearMultilineState() {
      multilineValueBuilder.setLength(0);
      currentMultilineKey = null;
      currentMultilineTarget = null;
    }

    void clearDescriptionState() {
      descBuilder.setLength(0);
      currentDescriptionKey = null;
    }

    void clearXmlState() {
      xmlBuilder.setLength(0);
    }
  }

  @Override
  public RecordFleakData extract(String raw) throws Exception {
    Objects.requireNonNull(raw, "Raw input cannot be null");

    final Map<String, Object> root = new HashMap<>();
    final ParserContext context = new ParserContext();

    try (BufferedReader reader = new BufferedReader(new StringReader(raw))) {
      String line;
      while ((line = reader.readLine()) != null) {
        processLine(line, context, root);
      }
    } catch (IOException e) {
      throw new Exception("Failed to parse input", e);
    }

    finalizeContent(root, context);
    addMetadata(root, raw);

    return (RecordFleakData) FleakData.wrap(root);
  }

  private void processLine(String line, ParserContext context, Map<String, Object> root) {
    context.state =
        switch (context.state) {
          case INIT -> handleInitState(line, root, context);
          case ROOT -> handleRootState(line, root, context);
          case NESTED -> handleNestedState(line, root, context);
          case GATHER_DESCRIPTION -> handleGatherDescriptionState(line, root, context);
          case GATHER_XML -> handleGatherXmlState(line, root, context);
          case GATHER_MULTILINE_VALUE -> handleGatherMultilineValueState(line, root, context);
        };
  }

  private ParserState handleInitState(
      String line, Map<String, Object> root, ParserContext context) {
    if (isKvPair(line)) {
      return handleKvPairLine(line, root, context, ParserState.ROOT);
    } else if (isKeyOnly(line)) {
      String key = parseKeyName(line);
      return handleKeyOnlyLine(key, root, context);
    }
    return ParserState.INIT;
  }

  private ParserState handleRootState(
      String line, Map<String, Object> root, ParserContext context) {
    if (isKvPair(line)) {
      return handleKvPairLine(line, root, context, ParserState.ROOT);
    } else if (isKeyOnly(line)) {
      String key = parseKeyName(line);
      return handleKeyOnlyLine(key, root, context);
    } else if (StringUtils.isNotBlank(line)) {
      return startDescriptionGathering(line, context);
    }
    return ParserState.ROOT;
  }

  private ParserState handleNestedState(
      String line, Map<String, Object> root, ParserContext context) {
    if (StringUtils.isBlank(line)) {
      return ParserState.NESTED;
    }

    validateNestedContext(context);

    if (isIndentedKvPair(line)) {
      return handleKvPairLine(line, context.nested, context, ParserState.NESTED);
    } else if (isNonIndentedKvPair(line)) {
      context.nested = null;
      return handleKvPairLine(line, root, context, ParserState.ROOT);
    } else if (isKeyOnly(line)) {
      return handleKeyOnlyInNestedState(line, root, context);
    } else {
      context.nested = null;
      return startDescriptionGathering(line, context);
    }
  }

  private ParserState handleGatherDescriptionState(
      String line, Map<String, Object> root, ParserContext context) {
    if (isKeyOnly(line)) {
      String key = parseKeyName(line);
      saveDescription(root, context);
      return handleKeyOnlyLine(key, root, context);
    } else if (isNonIndentedKvPair(line)) {
      saveDescription(root, context);
      return handleKvPairLine(line, root, context, ParserState.ROOT);
    } else if (StringUtils.isNotBlank(line)) {
      context.descBuilder.append(line).append("\n");
    }
    return ParserState.GATHER_DESCRIPTION;
  }

  private ParserState handleGatherXmlState(
      String line, Map<String, Object> root, ParserContext context) {
    if (isKeyOnly(line) && !line.trim().startsWith("<")) {
      String key = parseKeyName(line);
      saveXmlContent(root, context);
      return handleKeyOnlyLine(key, root, context);
    } else if (isNonIndentedKvPair(line) && !line.trim().startsWith("<")) {
      saveXmlContent(root, context);
      return handleKvPairLine(line, root, context, ParserState.ROOT);
    } else if (StringUtils.isNotBlank(line)) {
      context.xmlBuilder.append(line).append("\n");
    }
    return ParserState.GATHER_XML;
  }

  private ParserState handleGatherMultilineValueState(
      String line, Map<String, Object> root, ParserContext context) {
    if (isMultilineValueContinuation(line)) {
      context.multilineValueBuilder.append("\n").append(line.trim());
      return ParserState.GATHER_MULTILINE_VALUE;
    } else if (isIndentedKvPair(line)) {
      return handleIndentedKvInMultilineState(line, context);
    } else if (isKeyOnly(line)) {
      saveMultilineValue(context);
      String key = parseKeyName(line);
      return handleKeyOnlyLine(key, root, context);
    } else if (isNonIndentedKvPair(line)) {
      saveMultilineValue(context);
      context.nested = null;
      return handleKvPairLine(line, root, context, ParserState.ROOT);
    } else if (StringUtils.isBlank(line)) {
      return ParserState.GATHER_MULTILINE_VALUE;
    } else {
      saveMultilineValue(context);
      context.nested = null;
      return startDescriptionGathering(line, context);
    }
  }

  // Extracted common methods to reduce duplication
  private ParserState handleKvPairLine(
      String line, Map<String, Object> target, ParserContext context, ParserState nextState) {
    Pair<String, String> kv = parseKv(line);
    target.put(kv.getKey(), kv.getValue());

    if (isPotentialMultilineValue(kv.getValue())) {
      setupMultilineValue(context, kv.getKey(), kv.getValue(), target);
      return ParserState.GATHER_MULTILINE_VALUE;
    }

    return nextState;
  }

  private ParserState startDescriptionGathering(String line, ParserContext context) {
    context.descBuilder.append(line).append("\n");
    context.currentDescriptionKey = FIELD_DESCRIPTION_LOWER;
    return ParserState.GATHER_DESCRIPTION;
  }

  private ParserState handleKeyOnlyInNestedState(
      String line, Map<String, Object> root, ParserContext context) {
    String key = parseKeyName(line);
    if (isDescriptionField(key)) {
      context.nested = null;
      context.currentDescriptionKey = key;
      return ParserState.GATHER_DESCRIPTION;
    } else {
      return getParserState(root, context, key);
    }
  }

  @NotNull
  private WindowsMultilineExtractionRule.ParserState getParserState(
      Map<String, Object> root, ParserContext context, String key) {
    if (FIELD_EVENT_XML.equals(key)) {
      context.nested = null;
      return ParserState.GATHER_XML;
    } else {
      Map<String, Object> nestedMap = new HashMap<>();
      root.put(key, nestedMap);
      context.nested = nestedMap;
      return ParserState.NESTED;
    }
  }

  private ParserState handleIndentedKvInMultilineState(String line, ParserContext context) {
    saveMultilineValue(context);
    Pair<String, String> kv = parseKv(line);

    if (context.nested != null) {
      context.nested.put(kv.getKey(), kv.getValue());
      if (isPotentialMultilineValue(kv.getValue())) {
        setupMultilineValue(context, kv.getKey(), kv.getValue(), context.nested);
        return ParserState.GATHER_MULTILINE_VALUE;
      }
      return ParserState.NESTED;
    } else {
      // No nested context, treat as description
      context.descBuilder.append(line).append("\n");
      context.currentDescriptionKey = FIELD_DESCRIPTION_LOWER;
      return ParserState.GATHER_DESCRIPTION;
    }
  }

  private ParserState handleKeyOnlyLine(
      String key, Map<String, Object> root, ParserContext context) {
    if (isDescriptionField(key)) {
      context.currentDescriptionKey = key;
      context.nested = null;
      return ParserState.GATHER_DESCRIPTION;
    } else return getParserState(root, context, key);
  }

  private void validateNestedContext(ParserContext context) {
    if (context.nested == null) {
      throw new IllegalStateException("Nested map should not be null in NESTED state");
    }
  }

  private void setupMultilineValue(
      ParserContext context, String key, String value, Map<String, Object> target) {
    context.currentMultilineKey = key;
    context.currentMultilineTarget = target;
    context.multilineValueBuilder.setLength(0);
    context.multilineValueBuilder.append(value);
  }

  private void saveDescription(Map<String, Object> root, ParserContext context) {
    if (context.descBuilder.isEmpty()) {
      return;
    }

    String key =
        context.currentDescriptionKey != null
            ? context.currentDescriptionKey
            : FIELD_DESCRIPTION_LOWER;
    String existingDesc = (String) root.get(key);
    String newDesc = context.descBuilder.toString().trim();

    if (StringUtils.isNotBlank(existingDesc)) {
      root.put(key, existingDesc + "\n" + newDesc);
    } else {
      root.put(key, newDesc);
    }
    context.clearDescriptionState();
  }

  private void saveXmlContent(Map<String, Object> root, ParserContext context) {
    if (!context.xmlBuilder.isEmpty()) {
      root.put(FIELD_EVENT_XML, context.xmlBuilder.toString().trim());
      context.clearXmlState();
    }
  }

  private void saveMultilineValue(ParserContext context) {
    if (!context.multilineValueBuilder.isEmpty()
        && context.currentMultilineTarget != null
        && context.currentMultilineKey != null) {
      context.currentMultilineTarget.put(
          context.currentMultilineKey, context.multilineValueBuilder.toString());
      context.clearMultilineState();
    }
  }

  private void finalizeContent(Map<String, Object> root, ParserContext context) {
    saveMultilineValue(context);
    saveDescription(root, context);
    saveXmlContent(root, context);
  }

  private void addMetadata(Map<String, Object> root, String raw) {
    root.put(FIELD_NAME_RAW, raw);
    if (timestampExtractor != null) {
      String tsStr = timestampExtractor.extractTimestampString(root);
      if (tsStr != null) {
        root.put(FIELD_NAME_TS, tsStr);
      }
    }
  }

  // Pattern matching methods
  private boolean isDescriptionField(String key) {
    return FIELD_DESCRIPTION.equals(key) || FIELD_DESCRIPTION_LOWER.equals(key);
  }

  private boolean isKvPair(String line) {
    return isNonIndentedKvPair(line) || isIndentedKvPair(line);
  }

  private boolean isNonIndentedKvPair(String line) {
    if (PATTERN_LINE_KEY_ONLY.matcher(line).matches()) {
      return false;
    }
    return PATTERN_LINE_NON_INDENTED_KV_PAIR.matcher(line).matches()
        || PATTERN_WINDOWS_EVENTVIEWER_KV.matcher(line).matches();
  }

  private boolean isIndentedKvPair(String line) {
    return PATTERN_LINE_INDENTED_KV_PAIR.matcher(line).matches();
  }

  private boolean isKeyOnly(String line) {
    return PATTERN_LINE_KEY_ONLY.matcher(line).matches();
  }

  private boolean isMultilineValueContinuation(String line) {
    return PATTERN_MULTILINE_VALUE_CONTINUATION.matcher(line).matches();
  }

  private boolean isPotentialMultilineValue(String value) {
    if (StringUtils.isBlank(value)) {
      return false;
    }

    // Check for patterns that indicate complete values
    if (containsCompleteValueIndicators(value)) {
      return false;
    }

    // Check for patterns that suggest multi-line values
    return PATTERN_PRIVILEGE_NAME.matcher(value).matches()
        || PATTERN_SINGLE_WORD.matcher(value).matches()
        || value.length() < MULTILINE_VALUE_MAX_LENGTH;
  }

  private boolean containsCompleteValueIndicators(String value) {
    return value.contains(BACKSLASH)
        || value.contains(AT_SYMBOL)
        || value.contains(PROTOCOL_SEPARATOR)
        || value.contains(EQUALS)
        || value.contains(COLON)
        || PATTERN_NUMBER.matcher(value).matches()
        || PATTERN_HEX.matcher(value).matches()
        || PATTERN_SID.matcher(value).matches()
        || PATTERN_GUID.matcher(value).matches()
        || value.length() > COMPLETE_VALUE_MAX_LENGTH;
  }

  private String parseKeyName(String line) {
    Matcher matcher = PATTERN_LINE_KEY_ONLY.matcher(line);
    Preconditions.checkArgument(
        matcher.matches(), "Line does not match key-only pattern: %s", line);
    return matcher.group(1).trim();
  }

  private Pair<String, String> parseKv(String line) {
    String trimmedLine = line.trim();

    // Try standard KV pattern first
    Matcher matcher = PATTERN_KV_PAIR.matcher(trimmedLine);
    if (matcher.matches()) {
      return Pair.of(matcher.group(1).trim(), matcher.group(2).trim());
    }

    // Try Windows Event Viewer format
    matcher = PATTERN_WINDOWS_EVENTVIEWER_KV.matcher(trimmedLine);
    if (matcher.matches()) {
      return Pair.of(matcher.group(1).trim(), matcher.group(2).trim());
    }

    // Fallback: split on first colon or equals
    int splitIndex = findSplitIndex(trimmedLine);
    Preconditions.checkArgument(splitIndex >= 0, "Cannot parse key-value pair: %s", line);

    String key = trimmedLine.substring(0, splitIndex).trim();
    String value = trimmedLine.substring(splitIndex + 1).trim();

    return Pair.of(key, value);
  }

  private int findSplitIndex(String line) {
    int colonIndex = line.indexOf(COLON);
    int equalsIndex = line.indexOf(EQUALS);

    if (colonIndex >= 0 && equalsIndex >= 0) {
      return Math.min(colonIndex, equalsIndex);
    } else if (colonIndex >= 0) {
      return colonIndex;
    } else if (equalsIndex >= 0) {
      return equalsIndex;
    } else {
      return -1;
    }
  }

  public static TimestampExtractor createTimestampExtractor(
      WindowsMultilineExtractionConfig extractionConfig) {
    Objects.requireNonNull(extractionConfig, "Extraction config cannot be null");
    Objects.requireNonNull(
        extractionConfig.timestampLocationType, "Timestamp location type cannot be null");

    return switch (extractionConfig.timestampLocationType) {
      case FIRST_LINE -> new FirstLineTimestampExtractor();
      case FROM_FIELD -> createFromFieldExtractor(extractionConfig);
      case NO_TIMESTAMP -> null;
    };
  }

  private static FromFieldTimestampExtractor createFromFieldExtractor(
      WindowsMultilineExtractionConfig extractionConfig) {
    Objects.requireNonNull(
        extractionConfig.config, "Config map cannot be null for FROM_FIELD type");
    String targetField =
        (String) extractionConfig.config.get(FromFieldTimestampExtractor.CONFIG_TARGET_FIELD);
    Objects.requireNonNull(targetField, "Target field cannot be null for FROM_FIELD type");
    return new FromFieldTimestampExtractor(targetField);
  }

  static class FirstLineTimestampExtractor implements TimestampExtractor {
    @Override
    public String extractTimestampString(Map<String, Object> payload) {
      Objects.requireNonNull(payload, "Payload cannot be null");

      String raw = (String) payload.get(FIELD_NAME_RAW);
      if (raw == null) {
        return null;
      }
      String[] lines = raw.lines().toArray(String[]::new);
      return lines.length > 0 ? lines[0] : null;
    }
  }

  static class FromFieldTimestampExtractor implements TimestampExtractor {
    static final String CONFIG_TARGET_FIELD = "target_field";
    private final String fieldName;

    FromFieldTimestampExtractor(String fieldName) {
      this.fieldName = Objects.requireNonNull(fieldName, "Field name cannot be null");
    }

    @Override
    public String extractTimestampString(Map<String, Object> payload) {
      Objects.requireNonNull(payload, "Payload cannot be null");
      Object val = payload.get(fieldName);
      return val instanceof String stringVal ? stringVal : null;
    }
  }
}
