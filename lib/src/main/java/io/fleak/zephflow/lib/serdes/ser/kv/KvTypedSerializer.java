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
package io.fleak.zephflow.lib.serdes.ser.kv;

import io.fleak.zephflow.lib.serdes.ser.MultipleEventsTypedSerializer;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class KvTypedSerializer extends MultipleEventsTypedSerializer<Map<String, Object>> {

  private static final Pattern UNCLEAN_KEY_CHARS = Pattern.compile("[^A-Za-z0-9_]");
  private static final Pattern KEY_BREAKING_CHARS = Pattern.compile("[\\s=\"]");
  private static final Comparator<String> KEY_ORDER =
      String.CASE_INSENSITIVE_ORDER.thenComparing(Comparator.naturalOrder());

  private final KvSerializerOptions options;

  public KvTypedSerializer(KvSerializerOptions options) {
    this.options = Objects.requireNonNullElse(options, KvSerializerOptions.DEFAULT);
  }

  @Override
  protected byte[] serializeToMultipleTypedEvent(List<Map<String, Object>> typedValues) {
    StringJoiner lines = new StringJoiner("\n");
    for (Map<String, Object> event : typedValues) {
      String line = toLine(event);
      if (!line.isEmpty()) {
        lines.add(line);
      }
    }
    return lines.toString().getBytes(StandardCharsets.UTF_8);
  }

  String toLine(Map<String, Object> event) {
    Collection<String> keys = options.hasPinnedFields() ? options.fields() : sortedKeys(event);
    StringJoiner pairs = new StringJoiner(options.pairDelimiter());
    for (String key : keys) {
      Object value = event.get(key);
      if (value == null) {
        continue;
      }
      pairs.add(formatKey(key) + "=" + formatValue(value));
    }
    return pairs.toString();
  }

  private static Collection<String> sortedKeys(Map<String, Object> event) {
    TreeSet<String> keys = new TreeSet<>(KEY_ORDER);
    keys.addAll(event.keySet());
    return keys;
  }

  private String formatKey(String key) {
    if (options.cleanFields()) {
      key = UNCLEAN_KEY_CHARS.matcher(key).replaceAll("_");
    } else {
      key = KEY_BREAKING_CHARS.matcher(key).replaceAll("_").replace(options.pairDelimiter(), "_");
    }
    return key.isEmpty() ? "_" : key;
  }

  private String formatValue(Object value) {
    String text =
        value instanceof Map<?, ?> || value instanceof Collection<?>
            ? JsonUtils.toJsonString(value)
            : String.valueOf(value);
    return needsQuotes(text) ? quote(text) : text;
  }

  private boolean needsQuotes(String text) {
    if (text.isEmpty() || text.contains(options.pairDelimiter())) {
      return true;
    }
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (Character.isWhitespace(c) || c == '"' || c == '=' || c == '\\') {
        return true;
      }
    }
    return false;
  }

  private static String quote(String text) {
    StringBuilder sb = new StringBuilder(text.length() + 2).append('"');
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        default -> sb.append(c);
      }
    }
    return sb.append('"').toString();
  }
}
