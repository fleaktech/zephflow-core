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
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class KvTypedSerializer extends MultipleEventsTypedSerializer<Map<String, Object>> {

  private static final Pattern UNCLEAN_KEY_CHARS = Pattern.compile("[^A-Za-z0-9_]");
  private static final Pattern KEY_BREAKING_CHARS = Pattern.compile("[\\s=\"]");
  private static final Pattern SAFE_VALUE = Pattern.compile("[A-Za-z0-9_.:/-]+");
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
      lines.add(toLine(event));
    }
    return lines.toString().getBytes(StandardCharsets.UTF_8);
  }

  public String toLine(Map<String, Object> event) {
    Collection<String> keys = options.hasPinnedFields() ? options.fields() : sortedKeys(event);
    StringJoiner pairs = new StringJoiner(options.pairDelimiter());
    Set<String> written = new HashSet<>();
    for (String key : keys) {
      Object value = event.get(key);
      if (value == null) {
        continue;
      }
      pairs.add(uniqueKey(formatKey(key), written) + "=" + formatValue(value));
    }
    if (written.isEmpty()) {
      throw new IllegalArgumentException(
          options.hasPinnedFields()
              ? "event has none of the configured KV fields"
              : "event has no non-null fields to write as KV");
    }
    return pairs.toString();
  }

  private static Collection<String> sortedKeys(Map<String, Object> event) {
    TreeSet<String> keys = new TreeSet<>(KEY_ORDER);
    keys.addAll(event.keySet());
    return keys;
  }

  private static String uniqueKey(String key, Set<String> written) {
    String candidate = key;
    for (int n = 2; !written.add(candidate); n++) {
      candidate = key + "_" + n;
    }
    return candidate;
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
    String text = String.valueOf(value);
    return SAFE_VALUE.matcher(text).matches() ? text : quote(text);
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
