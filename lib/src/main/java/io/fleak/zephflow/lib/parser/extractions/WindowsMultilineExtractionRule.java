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
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Windows multiline extraction rule for parsing structured Windows event logs. Supports nested
 * key-value pairs, description text, and XML content.
 *
 * <p>Created by bolei on 2/1/25
 */
public record WindowsMultilineExtractionRule(TimestampExtractor timestampExtractor)
    implements ExtractionRule {

  // Enhanced regex patterns to handle different Windows log formats
  private static final String REGEX_KEY = "[\\w\\s().-]+";
  private static final String REGEX_VALUE = ".+";

  // Pattern for key-value pairs with flexible separators
  private static final String REGEX_KV_PAIR =
      String.format("(%s)[:=]\\s*(%s)", REGEX_KEY, REGEX_VALUE);

  private static final Pattern PATTERN_KV_PAIR = Pattern.compile(REGEX_KV_PAIR);

  // Patterns for different line types
  private static final Pattern PATTERN_LINE_NON_INDENTED_KV_PAIR = Pattern.compile(REGEX_KV_PAIR);
  private static final Pattern PATTERN_LINE_INDENTED_KV_PAIR =
      Pattern.compile("\\s+" + REGEX_KV_PAIR);

  // Enhanced pattern for key-only lines
  private static final Pattern PATTERN_LINE_KEY_ONLY = Pattern.compile("(" + REGEX_KEY + "):\\s*$");

  // Pattern to detect Windows Event Viewer format lines
  private static final Pattern PATTERN_WINDOWS_EVENTVIEWER_KV =
      Pattern.compile("([\\w\\s]+):\\s+(.+)");

  // Constants for special field names
  private static final String FIELD_DESCRIPTION = "Description";
  private static final String FIELD_DESCRIPTION_LOWER = "description";
  private static final String FIELD_EVENT_XML = "Event Xml";

  /** Parser states for handling different sections of Windows event logs. */
  enum ParserState {
    /** Initial state when starting to parse */
    INIT,
    /** Parsing root-level key-value pairs */
    ROOT,
    /** Parsing nested/indented key-value pairs */
    NESTED,
    /** Gathering description text */
    GATHER_DESCRIPTION,
    /** Gathering XML content */
    GATHER_XML
  }

  /** Context class to maintain parser state between line processing. */
  private static class ParserContext {
    ParserState state = ParserState.INIT;
    Map<String, Object> nested = null;
    final StringBuilder descBuilder = new StringBuilder();
    final StringBuilder xmlBuilder = new StringBuilder();
    String currentDescriptionKey = null;
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
    }

    // Handle any remaining content
    finalizeContent(root, context);

    // Add metadata
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
        };
  }

  private ParserState handleInitState(
      String line, Map<String, Object> root, ParserContext context) {
    if (isKvPair(line)) {
      Pair<String, String> kv = parseKv(line);
      root.put(kv.getKey(), kv.getValue());
      return ParserState.ROOT;
    } else if (isKeyOnly(line)) {
      String key = parseKeyName(line);
      return handleKeyOnlyLine(key, root, context);
    }
    return ParserState.INIT;
  }

  private ParserState handleRootState(
      String line, Map<String, Object> root, ParserContext context) {
    if (isKvPair(line)) {
      Pair<String, String> kv = parseKv(line);
      root.put(kv.getKey(), kv.getValue());
      return ParserState.ROOT;
    } else if (isKeyOnly(line)) {
      String key = parseKeyName(line);
      return handleKeyOnlyLine(key, root, context);
    } else if (!StringUtils.isBlank(line)) {
      // If we encounter unstructured text, treat as description
      context.descBuilder.append(line).append("\n");
      context.currentDescriptionKey = FIELD_DESCRIPTION_LOWER;
      return ParserState.GATHER_DESCRIPTION;
    }
    return ParserState.ROOT;
  }

  private ParserState handleNestedState(
      String line, Map<String, Object> root, ParserContext context) {
    if (StringUtils.isBlank(line)) {
      return ParserState.NESTED;
    }

    if (context.nested == null) {
      throw new IllegalStateException("Nested map should not be null in NESTED state");
    }

    if (isIndentedKvPair(line)) {
      Pair<String, String> kv = parseKv(line);
      context.nested.put(kv.getKey(), kv.getValue());
      return ParserState.NESTED;
    } else if (isNonIndentedKvPair(line)) {
      // Switch back to root level
      context.nested = null;
      Pair<String, String> kv = parseKv(line);
      root.put(kv.getKey(), kv.getValue());
      return ParserState.ROOT;
    } else if (isKeyOnly(line)) {
      String key = parseKeyName(line);
      return handleKeyOnlyLine(key, root, context);
    } else {
      // This is unstructured text, gather as description
      context.nested = null;
      context.descBuilder.append(line).append("\n");
      context.currentDescriptionKey = FIELD_DESCRIPTION_LOWER;
      return ParserState.GATHER_DESCRIPTION;
    }
  }

  private ParserState handleGatherDescriptionState(
      String line, Map<String, Object> root, ParserContext context) {
    if (isKeyOnly(line)) {
      String key = parseKeyName(line);
      // Save current description
      saveDescription(root, context);
      return handleKeyOnlyLine(key, root, context);
    } else if (isNonIndentedKvPair(line)) {
      // Save current description and switch to root level
      saveDescription(root, context);
      Pair<String, String> kv = parseKv(line);
      root.put(kv.getKey(), kv.getValue());
      return ParserState.ROOT;
    } else {
      // Continue gathering description
      if (!StringUtils.isBlank(line)) {
        context.descBuilder.append(line).append("\n");
      }
      return ParserState.GATHER_DESCRIPTION;
    }
  }

  private ParserState handleGatherXmlState(
      String line, Map<String, Object> root, ParserContext context) {
    // Check if we encounter a new section (not part of XML)
    if (isKeyOnly(line) && !line.trim().startsWith("<")) {
      String key = parseKeyName(line);
      // Save current XML
      saveXmlContent(root, context);
      return handleKeyOnlyLine(key, root, context);
    } else if (isNonIndentedKvPair(line) && !line.trim().startsWith("<")) {
      // Save current XML and switch to root level
      saveXmlContent(root, context);
      Pair<String, String> kv = parseKv(line);
      root.put(kv.getKey(), kv.getValue());
      return ParserState.ROOT;
    } else {
      // Continue gathering XML
      if (!StringUtils.isBlank(line)) {
        context.xmlBuilder.append(line).append("\n");
      }
      return ParserState.GATHER_XML;
    }
  }

  private ParserState handleKeyOnlyLine(
      String key, Map<String, Object> root, ParserContext context) {
    if (isDescriptionField(key)) {
      context.currentDescriptionKey = key;
      context.nested = null;
      return ParserState.GATHER_DESCRIPTION;
    } else if (FIELD_EVENT_XML.equals(key)) {
      context.nested = null;
      return ParserState.GATHER_XML;
    } else {
      // Create new nested section
      Map<String, Object> nestedMap = new HashMap<>();
      root.put(key, nestedMap);
      context.nested = nestedMap;
      return ParserState.NESTED;
    }
  }

  private void saveDescription(Map<String, Object> root, ParserContext context) {
    if (!context.descBuilder.isEmpty()) {
      String key =
          context.currentDescriptionKey != null
              ? context.currentDescriptionKey
              : FIELD_DESCRIPTION_LOWER;
      String existingDesc = (String) root.get(key);
      String newDesc = context.descBuilder.toString().trim();

      if (existingDesc != null && !existingDesc.isEmpty()) {
        root.put(key, existingDesc + "\n" + newDesc);
      } else {
        root.put(key, newDesc);
      }
      context.descBuilder.setLength(0); // Clear the builder
    }
  }

  private void saveXmlContent(Map<String, Object> root, ParserContext context) {
    if (!context.xmlBuilder.isEmpty()) {
      root.put(FIELD_EVENT_XML, context.xmlBuilder.toString().trim());
      context.xmlBuilder.setLength(0); // Clear the builder
    }
  }

  private void finalizeContent(Map<String, Object> root, ParserContext context) {
    // Handle any remaining description content
    saveDescription(root, context);

    // Handle any remaining XML content
    saveXmlContent(root, context);
  }

  private void addMetadata(Map<String, Object> root, String raw) {
    // Add raw data
    root.put(FIELD_NAME_RAW, raw);

    // Extract timestamp if extractor is provided
    if (timestampExtractor != null) {
      String tsStr = timestampExtractor.extractTimestampString(root);
      if (tsStr != null) {
        root.put(FIELD_NAME_TS, tsStr);
      }
    }
  }

  private boolean isDescriptionField(String key) {
    return FIELD_DESCRIPTION.equals(key) || FIELD_DESCRIPTION_LOWER.equals(key);
  }

  private boolean isKvPair(String line) {
    return isNonIndentedKvPair(line) || isIndentedKvPair(line);
  }

  private boolean isNonIndentedKvPair(String line) {
    // Handle both formats: "Key=Value" and "Key:      Value"
    // But exclude lines that are key-only (end with colon and whitespace only)
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
    int colonIndex = trimmedLine.indexOf(':');
    int equalsIndex = trimmedLine.indexOf('=');
    int splitIndex = -1;

    if (colonIndex >= 0 && equalsIndex >= 0) {
      splitIndex = Math.min(colonIndex, equalsIndex);
    } else if (colonIndex >= 0) {
      splitIndex = colonIndex;
    } else if (equalsIndex >= 0) {
      splitIndex = equalsIndex;
    }

    Preconditions.checkArgument(splitIndex >= 0, "Cannot parse key-value pair: %s", line);

    String key = trimmedLine.substring(0, splitIndex).trim();
    String value = trimmedLine.substring(splitIndex + 1).trim();

    return Pair.of(key, value);
  }

  public static TimestampExtractor createTimestampExtractor(
      WindowsMultilineExtractionConfig extractionConfig) {
    Objects.requireNonNull(extractionConfig, "Extraction config cannot be null");
    Objects.requireNonNull(
        extractionConfig.timestampLocationType, "Timestamp location type cannot be null");

    return switch (extractionConfig.timestampLocationType) {
      case FIRST_LINE -> new FirstLineTimestampExtractor();
      case FROM_FIELD -> {
        Objects.requireNonNull(
            extractionConfig.config, "Config map cannot be null for FROM_FIELD type");
        String targetField =
            (String) extractionConfig.config.get(FromFieldTimestampExtractor.CONFIG_TARGET_FIELD);
        Objects.requireNonNull(targetField, "Target field cannot be null for FROM_FIELD type");
        yield new FromFieldTimestampExtractor(targetField);
      }
      case NO_TIMESTAMP -> null;
    };
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
      if (val instanceof String stringVal) {
        return stringVal;
      }
      return null;
    }
  }
}
