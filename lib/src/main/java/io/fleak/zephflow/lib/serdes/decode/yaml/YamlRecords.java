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
 * Turns a constructed YAML document into a record. Aliases are expanded into separate copies, a
 * structure that contains itself is rejected, and expansion stops at {@link #MAX_VALUES} so that a
 * few aliases cannot multiply a small document into a huge record.
 */
final class YamlRecords {

  static final int MAX_VALUES = 1_000_000;

  private final Set<Object> path = Collections.newSetFromMap(new IdentityHashMap<>());
  private int values;

  private YamlRecords() {}

  static RecordFleakData toRecord(Object document) throws MalformedYamlException {
    if (!(document instanceof Map<?, ?> map)) {
      throw new MalformedYamlException(
          "expected a mapping but found " + document.getClass().getSimpleName());
    }
    return (RecordFleakData) new YamlRecords().convert(map);
  }

  private FleakData convert(Object value) throws MalformedYamlException {
    if (++values > MAX_VALUES) {
      throw new MalformedYamlException("document expands to more than " + MAX_VALUES + " values");
    }
    return switch (value) {
      case null -> null;
      case Map<?, ?> map -> {
        enter(map);
        Map<String, FleakData> payload = new HashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          payload.put(key(entry.getKey()), convert(entry.getValue()));
        }
        path.remove(map);
        yield new RecordFleakData(payload);
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
