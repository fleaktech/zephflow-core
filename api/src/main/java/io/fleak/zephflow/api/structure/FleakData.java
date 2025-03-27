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
// First, let's add Jackson serialization/deserialization support to the FleakData interface

package io.fleak.zephflow.api.structure;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import java.util.stream.Collectors;
import lombok.NonNull;

/**
 * Interface representing structured data in Fleak. Uses Jackson annotations for proper JSON
 * serialization/deserialization.
 */
@JsonSerialize(using = FleakDataSerializer.class)
@JsonDeserialize(using = FleakDataDeserializer.class)
public interface FleakData extends Comparable<FleakData> {

  ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  default Map<String, FleakData> getPayload() {
    throw new UnsupportedOperationException();
  }

  default String getStringValue() {
    throw new UnsupportedOperationException("trying to get string value from a non-string object");
  }

  default boolean isTrueValue() {
    throw new UnsupportedOperationException();
  }

  default double getNumberValue() {
    throw new UnsupportedOperationException();
  }

  default NumberPrimitiveFleakData.NumberType getNumberType() {
    throw new UnsupportedOperationException();
  }

  default List<FleakData> getArrayPayload() {
    throw new UnsupportedOperationException();
  }

  @Override
  default int compareTo(@NonNull FleakData o) {
    if (!(o instanceof PrimitiveFleakData)) {
      throw new RuntimeException("that object is not comparable: " + o);
    }
    if (!(this instanceof PrimitiveFleakData)) {
      throw new RuntimeException("this object is not comparable: " + this);
    }
    if (!this.getClass().equals(o.getClass())) {
      throw new RuntimeException(
          String.format("this (%s) and that (%s) are not of the same type", this, o));
    }
    return switch (this) {
      case StringPrimitiveFleakData ignored -> this.getStringValue().compareTo(o.getStringValue());
      case BooleanPrimitiveFleakData ignored ->
          Boolean.compare(this.isTrueValue(), o.isTrueValue());
      case NumberPrimitiveFleakData ignored ->
          Double.compare(this.getNumberValue(), o.getNumberValue());
      default ->
          throw new RuntimeException(
              String.format("cannot compare this (%s) and that (%s)", this, o));
    };
  }

  static boolean valueComparable(FleakData d1, FleakData d2) {
    if (!(d1 instanceof PrimitiveFleakData) || !(d2 instanceof PrimitiveFleakData)) {
      return false;
    }
    return Objects.equals(d1.getClass(), d2.getClass());
  }

  /**
   * FleakData wraps over java primitive data. This method returns the wrapped java data. This is
   * also used for JSON serialization.
   */
  @JsonValue
  Object unwrap();

  static FleakData wrap(Object obj) {

    switch (obj) {
      case null -> {
        return null;
      }
      case FleakData fleakData -> {
        return fleakData;
      }
      case Map<?, ?> map -> {
        var retMap = new HashMap<String, FleakData>();
        for (var entry : map.entrySet()) {
          retMap.put(entry.getKey().toString(), wrap(entry.getValue()));
        }
        return new RecordFleakData(retMap);
      }
      case ObjectNode n -> {
        return wrap(OBJECT_MAPPER.convertValue(n, new TypeReference<>() {}));
      }
      case Collection<?> l -> {
        return new ArrayFleakData(
            l.stream().map(FleakData::wrap).collect(Collectors.<FleakData>toList()));
      }
      case Integer n -> {
        return new NumberPrimitiveFleakData(
            n.doubleValue(), NumberPrimitiveFleakData.NumberType.INT);
      }
      case Long n -> {
        return new NumberPrimitiveFleakData(
            n.doubleValue(), NumberPrimitiveFleakData.NumberType.LONG);
      }
      case Float n -> {
        return new NumberPrimitiveFleakData(
            n.doubleValue(), NumberPrimitiveFleakData.NumberType.FLOAT);
      }
      case Number n -> {
        return new NumberPrimitiveFleakData(
            n.doubleValue(), NumberPrimitiveFleakData.NumberType.DOUBLE);
      }
      case Boolean b -> {
        return new BooleanPrimitiveFleakData(b);
      }
      default -> {}
    }

    return new StringPrimitiveFleakData(obj.toString());
  }
}
