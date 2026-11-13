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
package io.fleak.zephflow.lib.commands.databrickssink;

import static io.fleak.zephflow.api.ConfigurationValidationException.at;

import io.delta.kernel.types.*;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;

public class AvroToDeltaSchemaConverter {

  private static final Map<Schema.Type, DataType> PRIMITIVE_TYPES =
      Map.of(
          Schema.Type.STRING,
          StringType.STRING,
          Schema.Type.ENUM,
          StringType.STRING,
          Schema.Type.INT,
          IntegerType.INTEGER,
          Schema.Type.LONG,
          LongType.LONG,
          Schema.Type.FLOAT,
          FloatType.FLOAT,
          Schema.Type.DOUBLE,
          DoubleType.DOUBLE,
          Schema.Type.BOOLEAN,
          BooleanType.BOOLEAN);

  public static StructType convert(Schema avroSchema) {
    if (avroSchema.getType() != Schema.Type.RECORD) {
      throw at(
          ConfigurationPath.of("type"),
          new IllegalArgumentException(
              "Avro schema must be a RECORD type, got: " + avroSchema.getType()),
          "Avro schema must have type record.");
    }

    return convertRecord(avroSchema, ConfigurationPath.root());
  }

  public static StructType parse(String avroSchemaJson) {
    Schema avroSchema;
    try {
      avroSchema = new Schema.Parser().parse(avroSchemaJson);
    } catch (AvroRuntimeException failure) {
      throw at(
          ConfigurationPath.root(),
          failure,
          "Invalid Avro schema; expected a valid Avro record schema.");
    }
    return convert(avroSchema);
  }

  public static StructType parse(Map<String, Object> avroSchemaMap) {
    String json;
    try {
      json = JsonUtils.OBJECT_MAPPER.writeValueAsString(avroSchemaMap);
    } catch (Exception e) {
      throw at(
          ConfigurationPath.root(),
          new IllegalArgumentException(
              "Failed to convert avroSchema map to JSON: " + e.getMessage(), e),
          "Avro schema must be serializable as a JSON object.");
    }
    return parse(json);
  }

  private static DataType convertType(Schema avroSchema, ConfigurationPath path) {
    Schema effectiveSchema = unwrapNullable(avroSchema, path);
    ConfigurationPath effectivePath =
        avroSchema.getType() == Schema.Type.UNION
            ? path.index(avroSchema.getTypes().indexOf(effectiveSchema))
            : path;

    return switch (effectiveSchema.getType()) {
      case NULL ->
          throw at(
              effectivePath,
              new IllegalArgumentException("Standalone NULL type not supported"),
              "Standalone null is not supported; use a union containing one non-null type.");
      case BYTES, FIXED -> convertBinary(effectiveSchema, effectivePath);
      case ARRAY -> {
        DataType elementType =
            convertType(effectiveSchema.getElementType(), effectivePath.field("items"));
        boolean elementsNullable = isNullable(effectiveSchema.getElementType());
        yield new ArrayType(elementType, elementsNullable);
      }
      case MAP -> {
        DataType valueType =
            convertType(effectiveSchema.getValueType(), effectivePath.field("values"));
        boolean valuesNullable = isNullable(effectiveSchema.getValueType());
        yield new MapType(StringType.STRING, valueType, valuesNullable);
      }
      case RECORD -> convertRecord(effectiveSchema, effectivePath);
      case UNION ->
          throw at(
              effectivePath,
              new IllegalArgumentException(
                  "Complex union types are not supported (only nullable unions like [\"null\", \"type\"]): "
                      + avroSchema),
              "Union must contain exactly one non-null type.");
      default -> convertPrimitive(effectiveSchema);
    };
  }

  private static DataType convertPrimitive(Schema schema) {
    String logicalType = getLogicalType(schema);
    if (schema.getType() == Schema.Type.INT && "date".equals(logicalType)) return DateType.DATE;
    if (schema.getType() == Schema.Type.LONG
        && ("timestamp-millis".equals(logicalType) || "timestamp-micros".equals(logicalType)))
      return TimestampType.TIMESTAMP;
    return PRIMITIVE_TYPES.get(schema.getType());
  }

  private static StructType convertRecord(Schema schema, ConfigurationPath path) {
    List<StructField> fields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      DataType fieldType =
          convertType(field.schema(), path.field("fields").index(field.pos()).field("type"));
      fields.add(new StructField(field.name(), fieldType, isNullable(field.schema())));
    }
    return new StructType(fields);
  }

  private static DataType convertBinary(Schema effectiveSchema, ConfigurationPath path) {
    String logicalType = getLogicalType(effectiveSchema);
    if ("decimal".equals(logicalType)) {
      int precision = decimalProperty(effectiveSchema, "precision", 38, path);
      int scale = decimalProperty(effectiveSchema, "scale", 0, path);
      try {
        return new DecimalType(precision, scale);
      } catch (IllegalArgumentException failure) {
        throw at(
            path.field(precision < 0 || precision > 38 ? "precision" : "scale"),
            failure,
            "Decimal precision and scale must be between 0 and 38, with scale no greater than precision.");
      }
    }
    return BinaryType.BINARY;
  }

  private static int decimalProperty(
      Schema schema, String field, int fallback, ConfigurationPath path) {
    try {
      return schema.getObjectProp(field) != null
          ? ((Number) schema.getObjectProp(field)).intValue()
          : fallback;
    } catch (ClassCastException failure) {
      throw at(path.field(field), failure, "Decimal property must be a number.");
    }
  }

  private static boolean isNullable(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      return schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }
    return false;
  }

  private static Schema unwrapNullable(Schema schema, ConfigurationPath path) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }

    List<Schema> nonNullTypes =
        schema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).toList();

    if (nonNullTypes.size() == 1) {
      return nonNullTypes.getFirst();
    }

    throw at(
        path,
        new IllegalArgumentException(
            "Complex union types not supported (only nullable unions): " + schema),
        "Union must contain exactly one non-null type.");
  }

  private static String getLogicalType(Schema schema) {
    if (schema.getLogicalType() != null) {
      return schema.getLogicalType().getName();
    }
    return null;
  }
}
