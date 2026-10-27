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
package io.fleak.zephflow.lib.serdes.decode.otlp;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.MessageOrBuilder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A message as the OTLP/JSON encoding writes it, as a map: JSON field names, values at their
 * default omitted, 64-bit integers as strings, enums as numbers, trace and span ids as hex and
 * other bytes as base64.
 */
final class OtlpJsonMaps {

  private static final Set<String> HEX_FIELDS = Set.of("traceId", "spanId", "parentSpanId");

  private OtlpJsonMaps() {}

  static Map<String, Object> of(MessageOrBuilder message) {
    return of(message, Set.of());
  }

  /** Without the fields named in {@code skipped}, by JSON name. */
  static Map<String, Object> of(MessageOrBuilder message, Set<String> skipped) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
      if (skipped.contains(field.getJsonName())) {
        continue;
      }
      if (field.isRepeated()) {
        int count = message.getRepeatedFieldCount(field);
        if (count > 0) {
          List<Object> values = new ArrayList<>(count);
          for (int i = 0; i < count; i++) {
            values.add(value(field, message.getRepeatedField(field, i)));
          }
          map.put(field.getJsonName(), values);
        }
      } else if (field.hasPresence()
          ? message.hasField(field)
          : !message.getField(field).equals(field.getDefaultValue())) {
        map.put(field.getJsonName(), value(field, message.getField(field)));
      }
    }
    return map;
  }

  private static Object value(FieldDescriptor field, Object value) {
    return switch (field.getType()) {
      case INT64, SINT64, SFIXED64 -> Long.toString((Long) value);
      case UINT64, FIXED64 -> Long.toUnsignedString((Long) value);
      case UINT32, FIXED32 -> Integer.toUnsignedLong((Integer) value);
      case FLOAT -> ((Float) value).doubleValue();
      case ENUM -> ((EnumValueDescriptor) value).getNumber();
      case BYTES -> bytes(field, (ByteString) value);
      case MESSAGE, GROUP -> of((MessageOrBuilder) value);
      case INT32, SINT32, SFIXED32, DOUBLE, BOOL, STRING -> value;
    };
  }

  private static String bytes(FieldDescriptor field, ByteString value) {
    return HEX_FIELDS.contains(field.getJsonName())
        ? HexFormat.of().formatHex(value.toByteArray())
        : Base64.getEncoder().encodeToString(value.toByteArray());
  }
}
