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

import io.delta.kernel.types.*;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;

public class AvroToDeltaSchemaConverter {

  public static StructType convert(Schema avroSchema) {
    if (avroSchema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException(
          "Avro schema must be a RECORD type, got: " + avroSchema.getType());
    }

    List<StructField> fields = new ArrayList<>();
    for (Schema.Field avroField : avroSchema.getFields()) {
      DataType deltaType = convertType(avroField.schema());
      boolean nullable = isNullable(avroField.schema());
      fields.add(new StructField(avroField.name(), deltaType, nullable));
    }

    return new StructType(fields);
  }

  public static StructType parse(String avroSchemaJson) {
    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
    return convert(avroSchema);
  }

  public static StructType parse(Map<String, Object> avroSchemaMap) {
    try {
      String json = JsonUtils.OBJECT_MAPPER.writeValueAsString(avroSchemaMap);
      return parse(json);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to convert avroSchema map to JSON", e);
    }
  }

  private static DataType convertType(Schema avroSchema) {
    Schema effectiveSchema = unwrapNullable(avroSchema);

    return switch (effectiveSchema.getType()) {
      case STRING, ENUM -> StringType.STRING;
      case NULL -> throw new IllegalArgumentException("Standalone NULL type not supported");
      case INT -> {
        String logicalType = getLogicalType(effectiveSchema);
        if ("date".equals(logicalType)) {
          yield DateType.DATE;
        }
        yield IntegerType.INTEGER;
      }
      case LONG -> {
        String logicalType = getLogicalType(effectiveSchema);
        if ("timestamp-millis".equals(logicalType) || "timestamp-micros".equals(logicalType)) {
          yield TimestampType.TIMESTAMP;
        }
        yield LongType.LONG;
      }
      case FLOAT -> FloatType.FLOAT;
      case DOUBLE -> DoubleType.DOUBLE;
      case BOOLEAN -> BooleanType.BOOLEAN;
      case BYTES -> {
        String logicalType = getLogicalType(effectiveSchema);
        if ("decimal".equals(logicalType)) {
          int precision =
              effectiveSchema.getObjectProp("precision") != null
                  ? ((Number) effectiveSchema.getObjectProp("precision")).intValue()
                  : 38;
          int scale =
              effectiveSchema.getObjectProp("scale") != null
                  ? ((Number) effectiveSchema.getObjectProp("scale")).intValue()
                  : 0;
          yield new DecimalType(precision, scale);
        }
        yield BinaryType.BINARY;
      }
      case ARRAY -> {
        DataType elementType = convertType(effectiveSchema.getElementType());
        boolean elementsNullable = isNullable(effectiveSchema.getElementType());
        yield new ArrayType(elementType, elementsNullable);
      }
      case MAP -> {
        DataType valueType = convertType(effectiveSchema.getValueType());
        boolean valuesNullable = isNullable(effectiveSchema.getValueType());
        yield new MapType(StringType.STRING, valueType, valuesNullable);
      }
      case RECORD -> {
        List<StructField> fields = new ArrayList<>();
        for (Schema.Field field : effectiveSchema.getFields()) {
          DataType fieldType = convertType(field.schema());
          boolean fieldNullable = isNullable(field.schema());
          fields.add(new StructField(field.name(), fieldType, fieldNullable));
        }
        yield new StructType(fields);
      }
      case FIXED -> {
        String logicalType = getLogicalType(effectiveSchema);
        if ("decimal".equals(logicalType)) {
          int precision =
              effectiveSchema.getObjectProp("precision") != null
                  ? ((Number) effectiveSchema.getObjectProp("precision")).intValue()
                  : 38;
          int scale =
              effectiveSchema.getObjectProp("scale") != null
                  ? ((Number) effectiveSchema.getObjectProp("scale")).intValue()
                  : 0;
          yield new DecimalType(precision, scale);
        }
        yield BinaryType.BINARY;
      }
      case UNION ->
          throw new IllegalArgumentException(
              "Complex union types are not supported (only nullable unions like [\"null\", \"type\"]): "
                  + avroSchema);
    };
  }

  private static boolean isNullable(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      return schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }
    return false;
  }

  private static Schema unwrapNullable(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) {
      return schema;
    }

    List<Schema> nonNullTypes =
        schema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).toList();

    if (nonNullTypes.size() == 1) {
      return nonNullTypes.get(0);
    }

    throw new IllegalArgumentException(
        "Complex union types not supported (only nullable unions): " + schema);
  }

  private static String getLogicalType(Schema schema) {
    if (schema.getLogicalType() != null) {
      return schema.getLogicalType().getName();
    }
    return null;
  }
}
