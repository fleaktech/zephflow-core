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
package io.fleak.zephflow.lib.serdes.decode.yaml;

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData.NumberType;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Turns a constructed YAML document into a record. Aliases are expanded into separate copies and a
 * structure that contains itself is rejected. Expansion is capped at the number of values an
 * alias-free document within the size limit could hold (one per two bytes), so aliases cannot make
 * a record larger than the limit already allows. Without a limit, the values copied through aliases
 * are capped at {@link #UNLIMITED_ALIAS_COPIES}, because their growth is exponential, not linear in
 * the input. Expanded nesting is capped at {@link YamlSupport#MAX_NESTING}, as source nesting is.
 */
final class YamlRecords {

  static final long UNLIMITED_ALIAS_COPIES = 1_000_000;

  private final Set<Object> path = Collections.newSetFromMap(new IdentityHashMap<>());
  private final Set<Object> converted = Collections.newSetFromMap(new IdentityHashMap<>());

  /** Aliases share one scalar object; its text is made once so copies share it too. */
  private final Map<Object, String> scalarText = new IdentityHashMap<>();

  private final long maxValues;
  private final long maxCopies;
  private long values;
  private long copies;
  private int copying;

  private YamlRecords(long maxValues, long maxCopies) {
    this.maxValues = maxValues;
    this.maxCopies = maxCopies;
  }

  /** {@code sizeLimit} is the byte limit the document is held to, 0 when unlimited. */
  static RecordFleakData toRecord(Object document, long sizeLimit) throws MalformedYamlException {
    if (!(document instanceof Map<?, ?> map)) {
      throw new MalformedYamlException(
          "expected a mapping but found " + document.getClass().getSimpleName());
    }
    YamlRecords records =
        sizeLimit > 0
            ? new YamlRecords(maxValues(sizeLimit), Long.MAX_VALUE)
            : new YamlRecords(Long.MAX_VALUE, UNLIMITED_ALIAS_COPIES);
    return (RecordFleakData) records.convert(map);
  }

  /** The value cap for a size limit, or the alias-copy cap without one. */
  static long maxValues(long sizeLimit) {
    return sizeLimit > 0 ? Math.max(1, sizeLimit / 2) : UNLIMITED_ALIAS_COPIES;
  }

  private FleakData convert(Object value) throws MalformedYamlException {
    if (++values > maxValues) {
      throw new MalformedYamlException("document expands to more than " + maxValues + " values");
    }
    if (copying > 0 && ++copies > maxCopies) {
      throw new MalformedYamlException(
          "aliases copy more than " + maxCopies + " values into the document");
    }
    return switch (value) {
      case null -> null;
      case Map<?, ?> map -> {
        boolean copy = enter(map);
        Map<String, FleakData> payload = new HashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          String key = key(entry.getKey());
          if (payload.containsKey(key)) {
            throw new MalformedYamlException("two keys map to the same name: " + key);
          }
          payload.put(key, convert(entry.getValue()));
        }
        leave(map, copy);
        yield new RecordFleakData(payload);
      }
      case Object[] items -> array(items, Arrays.asList(items));
      case Collection<?> items -> array(items, items);
      case Integer n -> new NumberPrimitiveFleakData(n, NumberType.LONG);
      case Long n -> new NumberPrimitiveFleakData(n, NumberType.LONG);
      case BigInteger n when n.bitLength() < 64 ->
          new NumberPrimitiveFleakData(n.longValue(), NumberType.LONG);
      case BigInteger n -> new StringPrimitiveFleakData(text(n));
      case Double n when !Double.isFinite(n) -> new StringPrimitiveFleakData(n.toString());
      case Number n -> new NumberPrimitiveFleakData(n.doubleValue(), NumberType.DOUBLE);
      case Boolean b -> new BooleanPrimitiveFleakData(b);
      case Date date -> new NumberPrimitiveFleakData(date.getTime(), NumberType.LONG);
      case LocalDate date -> new NumberPrimitiveFleakData(date.toEpochDay(), NumberType.LONG);
      case String text -> new StringPrimitiveFleakData(text);
      case byte[] bytes -> new StringPrimitiveFleakData(text(bytes));
      default -> new StringPrimitiveFleakData(text(value));
    };
  }

  private String text(Object scalar) {
    return scalarText.computeIfAbsent(
        scalar,
        s -> s instanceof byte[] bytes ? Base64.getEncoder().encodeToString(bytes) : s.toString());
  }

  private ArrayFleakData array(Object container, Collection<?> items)
      throws MalformedYamlException {
    boolean copy = enter(container);
    List<FleakData> array = new ArrayList<>(items.size());
    for (Object item : items) {
      array.add(convert(item));
    }
    leave(container, copy);
    return new ArrayFleakData(array);
  }

  /**
   * Returns whether the container was converted before, i.e. is reached again through an alias, so
   * that everything inside it is a copy.
   */
  private boolean enter(Object container) throws MalformedYamlException {
    if (!path.add(container)) {
      throw new MalformedYamlException("document contains itself through an alias");
    }
    if (path.size() > YamlSupport.MAX_NESTING) {
      throw new MalformedYamlException(
          "document nests deeper than " + YamlSupport.MAX_NESTING + " levels");
    }
    boolean copy = !converted.add(container);
    if (copy) {
      copying++;
    }
    return copy;
  }

  private void leave(Object container, boolean copy) {
    path.remove(container);
    if (copy) {
      copying--;
    }
  }

  private String key(Object key) throws MalformedYamlException {
    return switch (key) {
      case null -> "null";
      case Map<?, ?> ignored -> throw new MalformedYamlException("mapping key is not a scalar");
      case Collection<?> ignored -> throw new MalformedYamlException("mapping key is not a scalar");
      case Date date -> String.valueOf(date.getTime());
      case LocalDate date -> String.valueOf(date.toEpochDay());
      case String text -> text;
      default -> text(key);
    };
  }
}
