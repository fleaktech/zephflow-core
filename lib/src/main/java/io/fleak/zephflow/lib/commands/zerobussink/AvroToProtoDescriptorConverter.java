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
package io.fleak.zephflow.lib.commands.zerobussink;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;

/**
 * Converts an Avro table schema into a runtime protobuf {@link DescriptorProto} that mirrors the
 * Delta table layout, and encodes individual records as {@link DynamicMessage}s — all without any
 * compile-time {@code .proto} code generation.
 *
 * <p>The protobuf is built in proto2 style ({@code optional} fields), matching Databricks Zerobus
 * expectations. Type mapping follows the documented Delta -&gt; protobuf table:
 *
 * <pre>
 *   INT/DATE        -&gt; int32        LONG/TIMESTAMP -&gt; int64
 *   FLOAT           -&gt; float        DOUBLE         -&gt; double
 *   BOOLEAN         -&gt; bool         STRING/ENUM    -&gt; string
 *   BYTES/FIXED     -&gt; bytes        RECORD         -&gt; nested message
 *   ARRAY           -&gt; repeated     MAP            -&gt; map&lt;string, V&gt; (map_entry message)
 * </pre>
 *
 * DECIMAL is not supported by Zerobus and is rejected here.
 */
public final class AvroToProtoDescriptorConverter {

  private static final String PROTO_PACKAGE = "io.fleak.zephflow.zerobus";
  private static final String ROOT_MESSAGE_NAME = "ZerobusRecord";

  private AvroToProtoDescriptorConverter() {}

  /** Parses and validates the Avro schema map, returning the root Avro {@link Schema}. */
  public static Schema parseAvro(Map<String, Object> avroSchemaMap) {
    String json;
    try {
      json = JsonUtils.OBJECT_MAPPER.writeValueAsString(avroSchemaMap);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert avroSchema map to JSON: " + e.getMessage(), e);
    }
    Schema schema = new Schema.Parser().parse(json);
    if (schema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException(
          "Avro schema must be a RECORD type, got: " + schema.getType());
    }
    return schema;
  }

  /**
   * Builds the protobuf {@link DescriptorProto} (with nested types) for {@code createProtoStream}.
   */
  public static DescriptorProto toDescriptorProto(
      Map<String, Object> avroSchemaMap, String tableName) {
    Schema avroSchema = parseAvro(avroSchemaMap);
    return buildMessage(
        ROOT_MESSAGE_NAME, "." + PROTO_PACKAGE + "." + ROOT_MESSAGE_NAME, avroSchema);
  }

  /**
   * Builds a usable {@link Descriptors.Descriptor} from a {@link DescriptorProto} so records can be
   * encoded as {@link DynamicMessage}s.
   */
  public static Descriptors.Descriptor toDescriptor(DescriptorProto messageProto) {
    FileDescriptorProto fileProto =
        FileDescriptorProto.newBuilder()
            .setName("zerobus_dynamic.proto")
            .setPackage(PROTO_PACKAGE)
            .setSyntax("proto2")
            .addMessageType(messageProto)
            .build();
    try {
      Descriptors.FileDescriptor fileDescriptor =
          Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[] {});
      return fileDescriptor.findMessageTypeByName(messageProto.getName());
    } catch (Descriptors.DescriptorValidationException e) {
      throw new IllegalArgumentException(
          "Failed to build protobuf descriptor from Avro schema: " + e.getMessage(), e);
    }
  }

  /** Encodes a single record (an unwrapped {@code Map}) as a {@link DynamicMessage}. */
  public static DynamicMessage toDynamicMessage(
      Map<String, Object> record, Schema avroSchema, Descriptors.Descriptor descriptor) {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    for (Schema.Field avroField : avroSchema.getFields()) {
      Object value = record.get(avroField.name());
      if (value == null) {
        // A missing/null value for a non-nullable column would otherwise be silently encoded as a
        // proto default and counted as a success. Fail loudly so the bad record is reported,
        // rather than letting Databricks reject (or worse, accept a NULL it shouldn't) later.
        if (!isNullable(avroField.schema())) {
          throw new IllegalArgumentException(
              "Missing value for non-nullable Zerobus field: " + avroField.name());
        }
        continue;
      }
      Descriptors.FieldDescriptor fd = descriptor.findFieldByName(avroField.name());
      if (fd == null) {
        continue;
      }
      Schema effective = unwrapNullable(avroField.schema());
      setField(builder, fd, effective, value);
    }
    return builder.build();
  }

  /** An Avro field is nullable iff it's a union containing {@code null}. */
  private static boolean isNullable(Schema schema) {
    return schema.getType() == Schema.Type.UNION
        && schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
  }

  // ---------------------------------------------------------------------------
  // Descriptor construction
  // ---------------------------------------------------------------------------

  /**
   * Builds a protobuf message for {@code recordSchema}. {@code qualifiedName} is the
   * fully-qualified proto name of this message (e.g. {@code
   * .io.fleak.zephflow.zerobus.ZerobusRecord.AddressType}), used to qualify the {@code type_name}
   * of any further nested message fields.
   */
  private static DescriptorProto buildMessage(
      String messageName, String qualifiedName, Schema recordSchema) {
    DescriptorProto.Builder message = DescriptorProto.newBuilder().setName(messageName);
    int fieldNumber = 1;
    for (Schema.Field avroField : recordSchema.getFields()) {
      addField(message, qualifiedName, avroField.name(), avroField.schema(), fieldNumber++);
    }
    return message.build();
  }

  private static void addField(
      DescriptorProto.Builder message,
      String enclosingQualifiedName,
      String fieldName,
      Schema fieldSchema,
      int fieldNumber) {
    Schema effective = unwrapNullable(fieldSchema);
    FieldDescriptorProto.Builder field =
        FieldDescriptorProto.newBuilder().setName(fieldName).setNumber(fieldNumber);

    switch (effective.getType()) {
      case ARRAY -> {
        field.setLabel(FieldDescriptorProto.Label.LABEL_REPEATED);
        Schema element = unwrapNullable(effective.getElementType());
        applyType(message, enclosingQualifiedName, field, fieldName, element);
      }
      case MAP -> {
        // map<string, V> => repeated <FieldName>Entry with map_entry option.
        String entryName = capitalize(fieldName) + "Entry";
        Schema valueSchema = unwrapNullable(effective.getValueType());
        message.addNestedType(
            buildMapEntry(message, enclosingQualifiedName, entryName, fieldName, valueSchema));
        field
            .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
            .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
            .setTypeName(enclosingQualifiedName + "." + entryName);
      }
      default -> {
        // Mirror the Databricks SDK's GenerateProto tool: non-nullable Delta columns become proto2
        // `required`, nullable ones `optional`. Unity Catalog validates the stream descriptor
        // against the target table; declaring a NOT NULL column as optional can fail stream
        // creation or weaken the schema contract.
        field.setLabel(
            isNullable(fieldSchema)
                ? FieldDescriptorProto.Label.LABEL_OPTIONAL
                : FieldDescriptorProto.Label.LABEL_REQUIRED);
        applyType(message, enclosingQualifiedName, field, fieldName, effective);
      }
    }
    message.addField(field.build());
  }

  /** Sets the protobuf type on a (scalar or message) field, registering nested types as needed. */
  private static void applyType(
      DescriptorProto.Builder message,
      String enclosingQualifiedName,
      FieldDescriptorProto.Builder field,
      String fieldName,
      Schema effective) {
    if (effective.getType() == Schema.Type.RECORD) {
      String nestedName = capitalize(fieldName) + "Type";
      String nestedQualified = enclosingQualifiedName + "." + nestedName;
      message.addNestedType(buildMessage(nestedName, nestedQualified, effective));
      field.setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(nestedQualified);
    } else {
      field.setType(scalarType(effective));
    }
  }

  private static DescriptorProto buildMapEntry(
      DescriptorProto.Builder parent,
      String enclosingQualifiedName,
      String entryName,
      String fieldName,
      Schema valueSchema) {
    FieldDescriptorProto keyField =
        FieldDescriptorProto.newBuilder()
            .setName("key")
            .setNumber(1)
            .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
            .setType(FieldDescriptorProto.Type.TYPE_STRING)
            .build();

    FieldDescriptorProto.Builder valueField =
        FieldDescriptorProto.newBuilder()
            .setName("value")
            .setNumber(2)
            .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    if (valueSchema.getType() == Schema.Type.RECORD) {
      String nestedName = capitalize(fieldName) + "ValueType";
      String nestedQualified = enclosingQualifiedName + "." + nestedName;
      parent.addNestedType(buildMessage(nestedName, nestedQualified, valueSchema));
      valueField.setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setTypeName(nestedQualified);
    } else {
      valueField.setType(scalarType(valueSchema));
    }

    return DescriptorProto.newBuilder()
        .setName(entryName)
        .setOptions(MessageOptions.newBuilder().setMapEntry(true).build())
        .addField(keyField)
        .addField(valueField.build())
        .build();
  }

  private static FieldDescriptorProto.Type scalarType(Schema effective) {
    return switch (effective.getType()) {
      case STRING, ENUM -> FieldDescriptorProto.Type.TYPE_STRING;
      case INT -> FieldDescriptorProto.Type.TYPE_INT32;
      case LONG -> FieldDescriptorProto.Type.TYPE_INT64;
      case FLOAT -> FieldDescriptorProto.Type.TYPE_FLOAT;
      case DOUBLE -> FieldDescriptorProto.Type.TYPE_DOUBLE;
      case BOOLEAN -> FieldDescriptorProto.Type.TYPE_BOOL;
      case BYTES, FIXED -> {
        if ("decimal".equals(logicalType(effective))) {
          // DECIMAL/NUMERIC is absent from the documented Databricks Zerobus
          // Delta->protobuf type-mapping table, so it cannot be ingested.
          throw new IllegalArgumentException(
              "DECIMAL columns are not in the Databricks Zerobus type mapping and cannot be ingested");
        }
        yield FieldDescriptorProto.Type.TYPE_BYTES;
      }
      default ->
          throw new IllegalArgumentException(
              "Unsupported Avro type for Zerobus protobuf encoding: " + effective.getType());
    };
  }

  // ---------------------------------------------------------------------------
  // Value encoding
  // ---------------------------------------------------------------------------

  private static void setField(
      DynamicMessage.Builder builder,
      Descriptors.FieldDescriptor fd,
      Schema effective,
      Object value) {
    if (fd.isMapField()) {
      // repeated entry message: build one entry per map item
      Descriptors.Descriptor entryType = fd.getMessageType();
      Descriptors.FieldDescriptor keyFd = entryType.findFieldByName("key");
      Descriptors.FieldDescriptor valueFd = entryType.findFieldByName("value");
      Schema valueSchema = unwrapNullable(effective.getValueType());
      Map<?, ?> map = (Map<?, ?>) value;
      for (Map.Entry<?, ?> e : map.entrySet()) {
        // A protobuf map entry cannot carry a null key or a null value — there is no lossless
        // representation. Skipping the entry would silently drop the key (changing the data while
        // still reporting success), so reject the record instead. (JSON encoding mode bypasses this
        // converter and preserves nulls faithfully; this limit is protobuf-only.)
        if (e.getKey() == null) {
          throw new IllegalArgumentException("Null key in map field: " + fd.getName());
        }
        if (e.getValue() == null) {
          throw new IllegalArgumentException(
              "Null map values are not supported by Zerobus protobuf encoding: " + fd.getName());
        }
        DynamicMessage.Builder entry = DynamicMessage.newBuilder(entryType);
        entry.setField(keyFd, e.getKey().toString());
        entry.setField(valueFd, convertValue(valueFd, valueSchema, e.getValue()));
        builder.addRepeatedField(fd, entry.build());
      }
      return;
    }

    if (fd.isRepeated()) {
      Schema element = unwrapNullable(effective.getElementType());
      for (Object item : (List<?>) value) {
        // A protobuf repeated field is a flat list with no null slots: ["a", null, "b"] could only
        // be encoded as ["a", "b"], changing length and element positions. Dropping the null is
        // silent corruption, so fail the record. (JSON mode preserves null elements faithfully.)
        if (item == null) {
          throw new IllegalArgumentException(
              "Null array elements are not supported by Zerobus protobuf encoding: "
                  + fd.getName());
        }
        builder.addRepeatedField(fd, convertValue(fd, element, item));
      }
      return;
    }

    builder.setField(fd, convertValue(fd, effective, value));
  }

  /** Converts a single value to the Java type protobuf expects for {@code fd}. */
  @SuppressWarnings("unchecked")
  private static Object convertValue(
      Descriptors.FieldDescriptor fd, Schema effective, Object value) {
    if (fd.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
      return toDynamicMessage((Map<String, Object>) value, effective, fd.getMessageType());
    }
    return switch (fd.getType()) {
      case INT32 -> toInt32(effective, value);
      case INT64 -> toInt64(effective, value);
      case FLOAT -> toFloat(value);
      case DOUBLE -> toDouble(value);
      case BOOL -> toBoolean(value);
      case STRING -> value.toString();
      case BYTES -> toByteString(value);
      default ->
          throw new IllegalArgumentException("Unsupported protobuf field type: " + fd.getType());
    };
  }

  private static int toInt32(Schema effective, Object value) {
    if ("date".equals(logicalType(effective)) && value instanceof CharSequence s) {
      return Math.toIntExact(LocalDate.parse(s.toString()).toEpochDay());
    }
    // Use exact conversions so out-of-range or fractional values fail loudly instead of being
    // silently truncated/wrapped (e.g. a Long > Integer.MAX_VALUE, or a Double like 42.9).
    if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long) {
      return Math.toIntExact(((Number) value).longValue());
    }
    if (value instanceof BigInteger bi) {
      return bi.intValueExact();
    }
    if (value instanceof BigDecimal bd) {
      return bd.intValueExact();
    }
    if (value instanceof Float || value instanceof Double) {
      double d = ((Number) value).doubleValue();
      if (d != Math.rint(d)) {
        throw new IllegalArgumentException("Non-integral value for int32 field: " + value);
      }
      return Math.toIntExact((long) d);
    }
    return Integer.parseInt(value.toString());
  }

  private static long toInt64(Schema effective, Object value) {
    String logical = logicalType(effective);

    // Databricks maps Delta TIMESTAMP -> protobuf int64 microseconds-since-epoch. The Avro logical
    // type tells us the unit of the *source* value so we coerce it to micros without loss.
    if ("timestamp-micros".equals(logical)) {
      if (value instanceof CharSequence s) {
        return instantToEpochMicros(Instant.parse(s.toString()));
      }
      // numeric value is already microseconds -> pass through unchanged
      return exactLong(value);
    }
    if ("timestamp-millis".equals(logical)) {
      if (value instanceof CharSequence s) {
        return instantToEpochMicros(Instant.parse(s.toString()));
      }
      // numeric value is milliseconds -> convert to micros
      return Math.multiplyExact(exactLong(value), 1000L);
    }

    // plain LONG, no logical type
    return exactLong(value);
  }

  /**
   * Converts a numeric/string value to a long with no silent loss. Like {@link #toInt32}, this
   * rejects fractional and out-of-range inputs instead of truncating or wrapping them (a corrupted
   * value would otherwise be counted as a successful write).
   */
  private static long exactLong(Object value) {
    if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long) {
      return ((Number) value).longValue();
    }
    if (value instanceof BigInteger bi) {
      return bi.longValueExact();
    }
    if (value instanceof BigDecimal bd) {
      return bd.longValueExact();
    }
    if (value instanceof Float || value instanceof Double) {
      // BigDecimal.valueOf(double).longValueExact() rejects both fractional values and values
      // outside the long range — a plain (long) cast would silently truncate or saturate.
      try {
        return BigDecimal.valueOf(((Number) value).doubleValue()).longValueExact();
      } catch (ArithmeticException e) {
        throw new IllegalArgumentException(
            "Non-integral or out-of-range value for int64 field: " + value, e);
      }
    }
    return Long.parseLong(value.toString());
  }

  /** Accepts only real booleans or the exact strings {@code "true"}/{@code "false"}. */
  private static boolean toBoolean(Object value) {
    if (value instanceof Boolean b) {
      return b;
    }
    if (value instanceof CharSequence s) {
      String normalized = s.toString().trim().toLowerCase(Locale.ROOT);
      if ("true".equals(normalized)) {
        return true;
      }
      if ("false".equals(normalized)) {
        return false;
      }
    }
    throw new IllegalArgumentException("Invalid boolean value for bool field: " + value);
  }

  /**
   * Narrows to {@code float}, rejecting non-numeric, non-finite, and out-of-range inputs. A raw
   * {@code ((Number) value).floatValue()} would silently turn e.g. {@code Double.MAX_VALUE} into
   * {@code Float.POSITIVE_INFINITY} and pass {@code NaN} straight through — a corrupted value
   * counted as a successful write.
   */
  private static float toFloat(Object value) {
    if (!(value instanceof Number n)) {
      throw new IllegalArgumentException("Invalid float value: " + value);
    }
    double d = n.doubleValue();
    if (!Double.isFinite(d) || d < -Float.MAX_VALUE || d > Float.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Out-of-range or non-finite value for float field: " + value);
    }
    float f = n.floatValue();
    // A non-zero source that narrows to 0.0f underflowed: writing 0 for a non-zero input is silent
    // corruption. (Checking the result directly is exact — it also accepts tiny values that round
    // up to the smallest subnormal float rather than to zero.)
    if (f == 0.0f && d != 0.0d) {
      throw new IllegalArgumentException("Underflow to zero for float field: " + value);
    }
    return f;
  }

  /** Widens to {@code double}, rejecting non-numeric and non-finite (NaN/Infinity) inputs. */
  private static double toDouble(Object value) {
    if (!(value instanceof Number n)) {
      throw new IllegalArgumentException("Invalid double value: " + value);
    }
    double d = n.doubleValue();
    if (!Double.isFinite(d)) {
      throw new IllegalArgumentException(
          "Out-of-range or non-finite value for double field: " + value);
    }
    return d;
  }

  /** Epoch microseconds, preserving sub-millisecond precision (unlike {@code toEpochMilli}). */
  private static long instantToEpochMicros(Instant instant) {
    return Math.addExact(
        Math.multiplyExact(instant.getEpochSecond(), 1_000_000L), instant.getNano() / 1_000L);
  }

  private static ByteString toByteString(Object value) {
    if (value instanceof byte[] b) {
      return ByteString.copyFrom(b);
    }
    if (value instanceof ByteString bs) {
      return bs;
    }
    if (value instanceof ByteBuffer buffer) {
      // slice() so we read the remaining bytes without disturbing the original buffer's position
      ByteBuffer copy = buffer.slice();
      byte[] bytes = new byte[copy.remaining()];
      copy.get(bytes);
      return ByteString.copyFrom(bytes);
    }
    if (value instanceof GenericFixed fixed) {
      return ByteString.copyFrom(fixed.bytes());
    }
    if (value instanceof CharSequence s) {
      // FleakData has no binary primitive: binary-in-a-record is carried as a base64 string (the
      // same convention as MiscUtils#toBase64String). Decode it rather than UTF-8-encoding the
      // literal text, which would silently corrupt the payload.
      try {
        return ByteString.copyFrom(Base64.getDecoder().decode(s.toString()));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid base64 value for bytes field: " + value, e);
      }
    }
    throw new IllegalArgumentException(
        "Unsupported value type for bytes field: " + value.getClass().getName());
  }

  // ---------------------------------------------------------------------------
  // Avro helpers
  // ---------------------------------------------------------------------------

  private static Schema unwrapNullable(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }
    List<Schema> nonNull =
        schema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).toList();
    if (nonNull.size() == 1) {
      return nonNull.getFirst();
    }
    throw new IllegalArgumentException(
        "Complex union types are not supported (only nullable unions): " + schema);
  }

  private static String logicalType(Schema schema) {
    return schema.getLogicalType() != null ? schema.getLogicalType().getName() : null;
  }

  private static String capitalize(String s) {
    if (s == null || s.isEmpty()) {
      return s;
    }
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }
}
