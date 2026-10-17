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
 * a record larger than the limit already allows. Without a limit there is no cap, as elsewhere.
 */
final class YamlRecords {

  private final Set<Object> path = Collections.newSetFromMap(new IdentityHashMap<>());
  private final long maxValues;
  private long values;

  private YamlRecords(long maxValues) {
    this.maxValues = maxValues;
  }

  /** {@code sizeLimit} is the byte limit the document is held to, 0 when unlimited. */
  static RecordFleakData toRecord(Object document, long sizeLimit) throws MalformedYamlException {
    if (!(document instanceof Map<?, ?> map)) {
      throw new MalformedYamlException(
          "expected a mapping but found " + document.getClass().getSimpleName());
    }
    long maxValues = sizeLimit > 0 ? Math.max(1, sizeLimit / 2) : Long.MAX_VALUE;
    return (RecordFleakData) new YamlRecords(maxValues).convert(map);
  }

  private FleakData convert(Object value) throws MalformedYamlException {
    if (++values > maxValues) {
      throw new MalformedYamlException("document expands to more than " + maxValues + " values");
    }
    return switch (value) {
      case null -> null;
      case Map<?, ?> map -> {
        enter(map);
        Map<String, FleakData> payload = new HashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          String key = key(entry.getKey());
          if (payload.containsKey(key)) {
            throw new MalformedYamlException("two keys map to the same name: " + key);
          }
          payload.put(key, convert(entry.getValue()));
        }
        path.remove(map);
        yield new RecordFleakData(payload);
      }
      case Object[] items -> {
        enter(items);
        List<FleakData> array = new ArrayList<>(items.length);
        for (Object item : items) {
          array.add(convert(item));
        }
        path.remove(items);
        yield new ArrayFleakData(array);
      }
      case Collection<?> items -> {
        enter(items);
        List<FleakData> array = new ArrayList<>(items.size());
        for (Object item : items) {
          array.add(convert(item));
        }
        path.remove(items);
        yield new ArrayFleakData(array);
      }
      case Integer n -> new NumberPrimitiveFleakData(n, NumberType.LONG);
      case Long n -> new NumberPrimitiveFleakData(n, NumberType.LONG);
      case BigInteger n when n.bitLength() < 64 ->
          new NumberPrimitiveFleakData(n.longValue(), NumberType.LONG);
      case BigInteger n -> new StringPrimitiveFleakData(n.toString());
      case Double n when !Double.isFinite(n) -> new StringPrimitiveFleakData(n.toString());
      case Number n -> new NumberPrimitiveFleakData(n.doubleValue(), NumberType.DOUBLE);
      case Boolean b -> new BooleanPrimitiveFleakData(b);
      case Date date -> new NumberPrimitiveFleakData(date.getTime(), NumberType.LONG);
      case LocalDate date -> new NumberPrimitiveFleakData(date.toEpochDay(), NumberType.LONG);
      case byte[] bytes -> new StringPrimitiveFleakData(Base64.getEncoder().encodeToString(bytes));
      default -> new StringPrimitiveFleakData(value.toString());
    };
  }

  private void enter(Object container) throws MalformedYamlException {
    if (!path.add(container)) {
      throw new MalformedYamlException("document contains itself through an alias");
    }
  }

  private static String key(Object key) throws MalformedYamlException {
    return switch (key) {
      case null -> "null";
      case Map<?, ?> ignored -> throw new MalformedYamlException("mapping key is not a scalar");
      case Collection<?> ignored -> throw new MalformedYamlException("mapping key is not a scalar");
      case Date date -> String.valueOf(date.getTime());
      case LocalDate date -> String.valueOf(date.toEpochDay());
      case byte[] bytes -> Base64.getEncoder().encodeToString(bytes);
      default -> key.toString();
    };
  }
}
