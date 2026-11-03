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
package io.fleak.zephflow.lib.parquet;

import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.apache.parquet.schema.Types.primitive;

import io.delta.kernel.types.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

/**
 * Delta kernel schema to Parquet schema, the same mapping delta-kernel-defaults 4.0.0 {@code
 * io.delta.kernel.defaults.internal.parquet.ParquetSchemaUtils} writes (Copyright (2023) The Delta
 * Lake Project Authors, Apache License 2.0), minus column-mapping field ids.
 */
final class ParquetSchemas {

  static final int DECIMAL_MAX_DIGITS_IN_INT = 9;
  static final int DECIMAL_MAX_DIGITS_IN_LONG = 18;

  /** Bytes a FIXED_LEN_BYTE_ARRAY needs to hold a decimal of each precision 0..38. */
  static final List<Integer> MAX_BYTES_PER_PRECISION;

  static {
    List<Integer> maxBytes = new ArrayList<>();
    for (int precision = 0; precision <= 38; precision++) {
      int numBytes = 1;
      while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
        numBytes++;
      }
      maxBytes.add(numBytes);
    }
    MAX_BYTES_PER_PRECISION = Collections.unmodifiableList(maxBytes);
  }

  private ParquetSchemas() {}

  static MessageType toParquetSchema(StructType structType) {
    return new MessageType("DefaultKernelSchema", fields(structType));
  }

  private static List<Type> fields(StructType structType) {
    List<Type> fields = new ArrayList<>();
    for (StructField field : structType.fields()) {
      fields.add(
          toParquetType(
              field.getDataType(), field.getName(), field.isNullable() ? OPTIONAL : REQUIRED));
    }
    return fields;
  }

  private static Type toParquetType(DataType dataType, String name, Repetition repetition) {
    if (dataType instanceof BooleanType) {
      return primitive(BOOLEAN, repetition).named(name);
    } else if (dataType instanceof ByteType
        || dataType instanceof ShortType
        || dataType instanceof IntegerType) {
      return primitive(INT32, repetition).named(name);
    } else if (dataType instanceof LongType) {
      return primitive(INT64, repetition).named(name);
    } else if (dataType instanceof FloatType) {
      // parquet 1.18 defaults floats to IEEE 754 order; delta-kernel wrote type-defined order.
      return primitive(FLOAT, repetition).columnOrder(ColumnOrder.typeDefined()).named(name);
    } else if (dataType instanceof DoubleType) {
      return primitive(DOUBLE, repetition).columnOrder(ColumnOrder.typeDefined()).named(name);
    } else if (dataType instanceof DecimalType decimal) {
      int precision = decimal.getPrecision();
      var annotation = decimalType(decimal.getScale(), precision);
      if (precision <= DECIMAL_MAX_DIGITS_IN_INT) {
        return primitive(INT32, repetition).as(annotation).named(name);
      } else if (precision <= DECIMAL_MAX_DIGITS_IN_LONG) {
        return primitive(INT64, repetition).as(annotation).named(name);
      }
      return primitive(FIXED_LEN_BYTE_ARRAY, repetition)
          .as(annotation)
          .length(MAX_BYTES_PER_PRECISION.get(precision))
          .named(name);
    } else if (dataType instanceof StringType) {
      return primitive(BINARY, repetition).as(LogicalTypeAnnotation.stringType()).named(name);
    } else if (dataType instanceof BinaryType) {
      return primitive(BINARY, repetition).named(name);
    } else if (dataType instanceof DateType) {
      return primitive(INT32, repetition).as(LogicalTypeAnnotation.dateType()).named(name);
    } else if (dataType instanceof TimestampType) {
      return primitive(INT64, repetition).as(timestampType(true, MICROS)).named(name);
    } else if (dataType instanceof TimestampNTZType) {
      return primitive(INT64, repetition).as(timestampType(false, MICROS)).named(name);
    } else if (dataType instanceof ArrayType array) {
      return Types.buildGroup(repetition)
          .as(LogicalTypeAnnotation.listType())
          .addField(
              Types.repeatedGroup()
                  .addField(
                      toParquetType(
                          array.getElementType(),
                          "element",
                          array.containsNull() ? OPTIONAL : REQUIRED))
                  .named("list"))
          .named(name);
    } else if (dataType instanceof MapType map) {
      return Types.buildGroup(repetition)
          .as(LogicalTypeAnnotation.mapType())
          .addField(
              Types.repeatedGroup()
                  .addField(toParquetType(map.getKeyType(), "key", REQUIRED))
                  .addField(
                      toParquetType(
                          map.getValueType(),
                          "value",
                          map.isValueContainsNull() ? OPTIONAL : REQUIRED))
                  .named("key_value"))
          .named(name);
    } else if (dataType instanceof StructType struct) {
      return new GroupType(repetition, name, fields(struct));
    }
    throw new UnsupportedOperationException(
        "Writing given type data to Parquet is not supported: " + dataType);
  }
}
