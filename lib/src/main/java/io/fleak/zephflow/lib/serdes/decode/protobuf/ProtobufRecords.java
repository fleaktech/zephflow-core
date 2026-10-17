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
package io.fleak.zephflow.lib.serdes.decode.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData.NumberType;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Protobuf messages to records, by field name. A field with presence (proto2, proto3 {@code
 * optional}, message fields, oneof members) appears only when set; a proto3 scalar without presence
 * always appears, with its default when unset. Unsigned integers are never negative, Timestamp and
 * Duration become milliseconds, bytes become base64, and enums their name (or number when unknown).
 */
final class ProtobufRecords {

  private ProtobufRecords() {}

  static RecordFleakData parse(Descriptor type, byte[] bytes) throws IOException {
    DynamicMessage message =
        DynamicMessage.newBuilder(type).mergeFrom(CodedInputStream.newInstance(bytes)).build();
    return message(message);
  }

  private static RecordFleakData message(Message message) {
    Map<String, FleakData> fields = new HashMap<>();
    for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
      if (field.isMapField()) {
        Map<String, FleakData> entries = new HashMap<>();
        for (Object entry : (List<?>) message.getField(field)) {
          Message pair = (Message) entry;
          Descriptor pairType = pair.getDescriptorForType();
          FieldDescriptor key = pairType.findFieldByNumber(1);
          FieldDescriptor value = pairType.findFieldByNumber(2);
          entries.put(mapKey(key, pair.getField(key)), value(value, pair.getField(value)));
        }
        fields.put(field.getName(), new RecordFleakData(entries));
      } else if (field.isRepeated()) {
        List<FleakData> items = new ArrayList<>();
        for (Object item : (List<?>) message.getField(field)) {
          items.add(value(field, item));
        }
        fields.put(field.getName(), new ArrayFleakData(items));
      } else if (!field.hasPresence() || message.hasField(field)) {
        fields.put(field.getName(), value(field, message.getField(field)));
      }
    }
    return new RecordFleakData(fields);
  }

  private static FleakData value(FieldDescriptor field, Object value) {
    return switch (field.getJavaType()) {
      case INT -> {
        int n = (Integer) value;
        yield number(unsigned32(field) ? Integer.toUnsignedLong(n) : n);
      }
      case LONG -> {
        long n = (Long) value;
        yield unsigned64(field) && n < 0
            ? new NumberPrimitiveFleakData(unsignedAsDouble(n), NumberType.LONG)
            : number(n);
      }
      case FLOAT -> real(((Float) value).doubleValue());
      case DOUBLE -> real((Double) value);
      case BOOLEAN -> new BooleanPrimitiveFleakData((Boolean) value);
      case STRING -> new StringPrimitiveFleakData((String) value);
      case BYTE_STRING ->
          new StringPrimitiveFleakData(
              Base64.getEncoder().encodeToString(((ByteString) value).toByteArray()));
      case ENUM -> {
        EnumValueDescriptor symbol = (EnumValueDescriptor) value;
        yield symbol.getIndex() < 0
            ? number(symbol.getNumber())
            : new StringPrimitiveFleakData(symbol.getName());
      }
      case MESSAGE -> wellKnownOrMessage((Message) value);
    };
  }

  private static FleakData wellKnownOrMessage(Message message) {
    String name = message.getDescriptorForType().getFullName();
    if (name.equals("google.protobuf.Timestamp") || name.equals("google.protobuf.Duration")) {
      Descriptor type = message.getDescriptorForType();
      long seconds = (Long) message.getField(type.findFieldByNumber(1));
      int nanos = (Integer) message.getField(type.findFieldByNumber(2));
      long millis =
          name.equals("google.protobuf.Timestamp")
              ? Math.floorDiv(nanos, 1_000_000)
              : nanos / 1_000_000;
      return number(Math.addExact(Math.multiplyExact(seconds, 1000L), millis));
    }
    return message(message);
  }

  private static String mapKey(FieldDescriptor key, Object value) {
    return switch (key.getJavaType()) {
      case INT ->
          unsigned32(key)
              ? Long.toString(Integer.toUnsignedLong((Integer) value))
              : value.toString();
      case LONG -> unsigned64(key) ? Long.toUnsignedString((Long) value) : value.toString();
      default -> value.toString();
    };
  }

  private static boolean unsigned32(FieldDescriptor field) {
    return field.getType() == FieldDescriptor.Type.UINT32
        || field.getType() == FieldDescriptor.Type.FIXED32;
  }

  private static boolean unsigned64(FieldDescriptor field) {
    return field.getType() == FieldDescriptor.Type.UINT64
        || field.getType() == FieldDescriptor.Type.FIXED64;
  }

  private static double unsignedAsDouble(long n) {
    return (double) (n >>> 1) * 2.0 + (n & 1);
  }

  private static FleakData number(long value) {
    return new NumberPrimitiveFleakData(value, NumberType.LONG);
  }

  private static FleakData real(double value) {
    return Double.isFinite(value)
        ? new NumberPrimitiveFleakData(value, NumberType.DOUBLE)
        : new StringPrimitiveFleakData(Double.toString(value));
  }
}
