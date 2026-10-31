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
package io.fleak.zephflow.api.structure;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Selects logical field populations using the field inspection path convention. Object names are
 * joined with dots and arrays do not add indices. Null and empty containers are present; containers
 * do not produce scalar values. The traversal is iterative and keeps only its current ancestry.
 */
public final class FieldPopulationSelector {
  private static final JsonFactory JSON = new JsonFactory();
  private static final ObjectMapper SCALARS = new ObjectMapper();

  private FieldPopulationSelector() {}

  /** Returns whether at least one location has this logical name, including null and containers. */
  public static boolean isPresent(FleakData record, String fieldName) {
    return walk(record, Objects.requireNonNull(fieldName), value -> true, false);
  }

  /**
   * Matches a JSON scalar by its exact persisted identity, without numeric comparison tolerance.
   */
  public static boolean matchesValue(FleakData record, String fieldName, String scalarJson) {
    String expected = requireScalarJson(scalarJson);
    return walk(
        record,
        Objects.requireNonNull(fieldName),
        located -> isScalar(located.value()) && expected.equals(scalarJson(located.value())),
        false);
  }

  /**
   * Emits scalar occurrences for one field in a staged JSON record and returns field presence.
   * Repeated occurrences are deliberately streamed; callers count each value once per record.
   */
  public static boolean visitJson(
      String recordJson, String fieldName, Consumer<String> scalarConsumer) {
    Objects.requireNonNull(fieldName);
    return visitJson(recordJson, fieldName::equals, scalarConsumer, false);
  }

  /** Resolves the original logical name without relying on a sanitized presentation label. */
  public static java.util.Optional<String> findFieldName(
      String recordJson, Predicate<String> matchesName) {
    String[] found = new String[1];
    visitJson(
        recordJson,
        path -> {
          if (!matchesName.test(path)) return false;
          found[0] = path;
          return true;
        },
        ignored -> {},
        true);
    return java.util.Optional.ofNullable(found[0]);
  }

  private static boolean visitJson(
      String recordJson,
      Predicate<String> selectedField,
      Consumer<String> scalarConsumer,
      boolean stopAtFirst) {
    Objects.requireNonNull(scalarConsumer);
    boolean present = false;
    try (JsonParser parser = JSON.createParser(recordJson)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        throw new IllegalArgumentException("A field population requires a JSON object record");
      }
      Deque<JsonFrame> stack = new ArrayDeque<>();
      stack.push(new JsonFrame("", true));
      JsonToken token;
      while ((token = parser.nextToken()) != null) {
        if (token == JsonToken.FIELD_NAME) {
          stack.peek().fieldName = parser.currentName();
        } else if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
          stack.pop();
          if (stack.isEmpty()) {
            if (parser.nextToken() != null) {
              throw new IllegalArgumentException("Expected exactly one JSON record");
            }
            return present;
          }
        } else {
          JsonFrame parent = stack.peek();
          String path = parent.object ? join(parent.path, parent.fieldName) : parent.path;
          boolean selected = selectedField.test(path);
          if (selected && stopAtFirst) return true;
          present |= selected;
          if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
            stack.push(new JsonFrame(path, token == JsonToken.START_OBJECT));
          } else if (selected) {
            scalarConsumer.accept(scalarToken(parser));
          }
        }
      }
      throw new IllegalArgumentException("Incomplete JSON record");
    } catch (IOException exception) {
      throw new UncheckedIOException("Failed to read a field population", exception);
    }
  }

  /**
   * Validates and normalizes the encoding of one scalar without parsing a number through double.
   */
  public static String requireScalarJson(String scalarJson) {
    Objects.requireNonNull(scalarJson, "Scalar JSON is required");
    try (JsonParser parser = JSON.createParser(scalarJson)) {
      JsonToken token = parser.nextToken();
      if (token == null || !token.isScalarValue()) {
        throw new IllegalArgumentException("Expected a single JSON scalar");
      }
      String normalized = scalarToken(parser);
      if (parser.nextToken() != null) {
        throw new IllegalArgumentException("Expected a single JSON scalar");
      }
      return normalized;
    } catch (IOException exception) {
      throw new IllegalArgumentException("Invalid JSON scalar", exception);
    }
  }

  /** Encodes a string as a JSON literal; callers may also use it for FEEL string arguments. */
  public static String quote(String value) {
    StringBuilder result = new StringBuilder(value.length() + 2).append('"');
    char[] quoted = JsonStringEncoder.getInstance().quoteAsString(value);
    for (int index = 0; index < quoted.length; index++) {
      char character = quoted[index];
      if (Character.isHighSurrogate(character)
          && index + 1 < quoted.length
          && Character.isLowSurrogate(quoted[index + 1])) {
        result.append(character).append(quoted[++index]);
      } else if (Character.isSurrogate(character)) {
        // Keep individual UTF-16 units intact through UTF-8, JDBC and FEEL literals.
        result.append("\\u");
        for (int shift = 12; shift >= 0; shift -= 4) {
          result.append(Character.forDigit((character >> shift) & 15, 16));
        }
      } else {
        result.append(character);
      }
    }
    return result.append('"').toString();
  }

  private static String scalarToken(JsonParser parser) throws IOException {
    return switch (parser.currentToken()) {
      case VALUE_STRING -> quote(parser.getText());
      case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> parser.getText();
      case VALUE_TRUE -> "true";
      case VALUE_FALSE -> "false";
      case VALUE_NULL -> "null";
      default -> throw new IllegalArgumentException("Expected a JSON scalar");
    };
  }

  private static String scalarJson(FleakData value) {
    if (value == null) return "null";
    if (value instanceof StringPrimitiveFleakData string) return quote(string.getStringValue());
    if (value instanceof BooleanPrimitiveFleakData bool)
      return Boolean.toString(bool.isTrueValue());
    if (value instanceof NumberPrimitiveFleakData number) {
      // unwrap preserves the LONG/DOUBLE representation used by staging and FleakDataSerializer.
      try {
        return SCALARS.writeValueAsString(number.unwrap());
      } catch (IOException exception) {
        throw new UncheckedIOException("Failed to encode a field scalar", exception);
      }
    }
    throw new IllegalArgumentException("Expected a scalar field value");
  }

  private static boolean isScalar(FleakData value) {
    return !(value instanceof RecordFleakData) && !(value instanceof ArrayFleakData);
  }

  /**
   * Visits every scalar occurrence (including null) of a logical field and returns its presence.
   * Locations distinguish object keys from array indices and are immutable snapshots. Containers
   * contribute to presence but are not emitted as scalar values.
   */
  public static boolean visitScalars(
      FleakData record,
      String fieldName,
      java.util.function.BiConsumer<java.util.List<PathSegment>, FleakData> scalarConsumer) {
    Objects.requireNonNull(scalarConsumer);
    boolean[] present = {false};
    walk(
        record,
        Objects.requireNonNull(fieldName),
        located -> {
          present[0] = true;
          if (isScalar(located.value())) {
            scalarConsumer.accept(located.location().segments(), located.value());
          }
          return false;
        },
        true);
    return present[0];
  }

  /** One physical step in a record, independent of the logical dotted field name. */
  public sealed interface PathSegment permits ObjectKey, ArrayIndex {}

  /** An object key, including keys that contain dots or look like array indices. */
  public record ObjectKey(String name) implements PathSegment {
    public ObjectKey {
      Objects.requireNonNull(name);
    }
  }

  /** A zero-based array position. */
  public record ArrayIndex(int index) implements PathSegment {
    public ArrayIndex {
      if (index < 0) throw new IllegalArgumentException("Array index must be nonnegative");
    }
  }

  private static boolean walk(
      FleakData root, String fieldName, Predicate<LocatedValue> selectedValue, boolean locate) {
    if (!(root instanceof RecordFleakData record)) return false;
    Deque<Iterator<LocatedValue>> stack = new ArrayDeque<>();
    stack.push(fields(record, "", locate ? new Location(null, null) : null));
    while (!stack.isEmpty()) {
      Iterator<LocatedValue> current = stack.peek();
      if (!current.hasNext()) {
        stack.pop();
        continue;
      }
      LocatedValue located = current.next();
      if (fieldName.equals(located.path()) && selectedValue.test(located)) return true;
      if (located.value() instanceof RecordFleakData child) {
        stack.push(fields(child, located.path(), located.location()));
      } else if (located.value() instanceof ArrayFleakData array) {
        Iterator<FleakData> elements = array.getArrayPayload().iterator();
        stack.push(
            new Iterator<>() {
              private int index;

              public boolean hasNext() {
                return elements.hasNext();
              }

              public LocatedValue next() {
                Location location = append(located.location(), new ArrayIndex(index++));
                return new LocatedValue(located.path(), elements.next(), location);
              }
            });
      }
    }
    return false;
  }

  private static Iterator<LocatedValue> fields(
      RecordFleakData record, String prefix, Location parent) {
    Iterator<Map.Entry<String, FleakData>> entries = record.getPayload().entrySet().iterator();
    return new Iterator<>() {
      public boolean hasNext() {
        return entries.hasNext();
      }

      public LocatedValue next() {
        Map.Entry<String, FleakData> entry = entries.next();
        return new LocatedValue(
            join(prefix, entry.getKey()),
            entry.getValue(),
            append(parent, new ObjectKey(entry.getKey())));
      }
    };
  }

  private static Location append(Location parent, PathSegment segment) {
    return parent == null ? null : new Location(parent, segment);
  }

  private record Location(Location parent, PathSegment segment) {
    private java.util.List<PathSegment> segments() {
      java.util.LinkedList<PathSegment> result = new java.util.LinkedList<>();
      for (Location current = this; current.segment != null; current = current.parent) {
        result.addFirst(current.segment);
      }
      return java.util.List.copyOf(result);
    }
  }

  private static String join(String prefix, String name) {
    return prefix.isEmpty() ? name : prefix + "." + name;
  }

  private record LocatedValue(String path, FleakData value, Location location) {}

  private static final class JsonFrame {
    private final String path;
    private final boolean object;
    private String fieldName;

    private JsonFrame(String path, boolean object) {
      this.path = path;
      this.object = object;
    }
  }
}
