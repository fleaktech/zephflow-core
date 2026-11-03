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

import static io.fleak.zephflow.lib.parquet.ParquetSchemas.MAX_BYTES_PER_PRECISION;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

/**
 * Writes column vectors to a {@link RecordConsumer} row by row. Ported from delta-kernel-defaults
 * 4.0.0 {@code io.delta.kernel.defaults.internal.parquet.ParquetColumnWriters}, Copyright (2023)
 * The Delta Lake Project Authors, Apache License 2.0, so the files keep the layout that writer
 * produced.
 */
final class ParquetColumnWriters {

  private ParquetColumnWriters() {}

  static ColumnWriter[] forBatch(ColumnarBatch batch) {
    StructType schema = batch.getSchema();
    ColumnWriter[] writers = new ColumnWriter[schema.length()];
    for (int i = 0; i < writers.length; i++) {
      writers[i] = create(schema.at(i).getName(), i, batch.getColumnVector(i));
    }
    return writers;
  }

  private static ColumnWriter[] forStruct(ColumnVector struct) {
    StructType schema = (StructType) struct.getDataType();
    ColumnWriter[] writers = new ColumnWriter[schema.length()];
    for (int i = 0; i < writers.length; i++) {
      writers[i] = create(schema.at(i).getName(), i, struct.getChild(i));
    }
    return writers;
  }

  private static ColumnWriter create(String name, int index, ColumnVector vector) {
    DataType type = vector.getDataType();
    if (type instanceof BooleanType) {
      return new ColumnWriter(name, index, vector, (c, r) -> c.addBoolean(vector.getBoolean(r)));
    } else if (type instanceof ByteType) {
      return new ColumnWriter(name, index, vector, (c, r) -> c.addInteger(vector.getByte(r)));
    } else if (type instanceof ShortType) {
      return new ColumnWriter(name, index, vector, (c, r) -> c.addInteger(vector.getShort(r)));
    } else if (type instanceof IntegerType || type instanceof DateType) {
      return new ColumnWriter(name, index, vector, (c, r) -> c.addInteger(vector.getInt(r)));
    } else if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return new ColumnWriter(name, index, vector, (c, r) -> c.addLong(vector.getLong(r)));
    } else if (type instanceof FloatType) {
      return new ColumnWriter(name, index, vector, (c, r) -> c.addFloat(vector.getFloat(r)));
    } else if (type instanceof DoubleType) {
      return new ColumnWriter(name, index, vector, (c, r) -> c.addDouble(vector.getDouble(r)));
    } else if (type instanceof StringType) {
      return new ColumnWriter(
          name,
          index,
          vector,
          (c, r) ->
              c.addBinary(
                  Binary.fromConstantByteArray(
                      vector.getString(r).getBytes(StandardCharsets.UTF_8))));
    } else if (type instanceof BinaryType) {
      return new ColumnWriter(
          name,
          index,
          vector,
          (c, r) -> c.addBinary(Binary.fromConstantByteArray(vector.getBinary(r))));
    } else if (type instanceof DecimalType decimal) {
      return decimalWriter(name, index, vector, decimal);
    } else if (type instanceof ArrayType) {
      return new ColumnWriter(name, index, vector, (c, r) -> writeArray(c, vector.getArray(r)));
    } else if (type instanceof MapType) {
      return new ColumnWriter(name, index, vector, (c, r) -> writeMap(c, vector.getMap(r)));
    } else if (type instanceof StructType) {
      ColumnWriter[] fields = forStruct(vector);
      return new ColumnWriter(
          name,
          index,
          vector,
          (c, r) -> {
            c.startGroup();
            for (ColumnWriter field : fields) {
              field.write(c, r);
            }
            c.endGroup();
          });
    }
    throw new IllegalArgumentException("Unsupported column vector type: " + type);
  }

  private static ColumnWriter decimalWriter(
      String name, int index, ColumnVector vector, DecimalType decimal) {
    int precision = decimal.getPrecision();
    int scale = decimal.getScale();
    if (precision <= ParquetSchemas.DECIMAL_MAX_DIGITS_IN_INT) {
      return new ColumnWriter(
          name,
          index,
          vector,
          (c, r) -> c.addInteger(vector.getDecimal(r).movePointRight(scale).intValue()));
    } else if (precision <= ParquetSchemas.DECIMAL_MAX_DIGITS_IN_LONG) {
      return new ColumnWriter(
          name,
          index,
          vector,
          (c, r) -> c.addLong(vector.getDecimal(r).movePointRight(scale).longValue()));
    }
    int numBytes = MAX_BYTES_PER_PRECISION.get(precision);
    byte[] buffer = new byte[numBytes];
    return new ColumnWriter(
        name,
        index,
        vector,
        (c, r) -> {
          BigDecimal value = vector.getDecimal(r);
          byte[] bytes = value.unscaledValue().toByteArray();
          if (bytes.length == numBytes) {
            c.addBinary(Binary.fromReusedByteArray(bytes));
            return;
          }
          // Sign-extend into the fixed width.
          byte sign = bytes[0] < 0 ? (byte) -1 : (byte) 0;
          Arrays.fill(buffer, 0, numBytes - bytes.length, sign);
          System.arraycopy(bytes, 0, buffer, numBytes - bytes.length, bytes.length);
          c.addBinary(Binary.fromReusedByteArray(buffer));
        });
  }

  private static void writeArray(RecordConsumer consumer, ArrayValue array) {
    consumer.startGroup();
    if (array.getSize() > 0) {
      consumer.startField("list", 0);
      ColumnVector elements = array.getElements();
      ColumnWriter element = create("element", 0, elements);
      for (int i = 0; i < array.getSize(); i++) {
        consumer.startGroup();
        element.write(consumer, i);
        consumer.endGroup();
      }
      consumer.endField("list", 0);
    }
    consumer.endGroup();
  }

  private static void writeMap(RecordConsumer consumer, MapValue map) {
    consumer.startGroup();
    if (map.getSize() > 0) {
      consumer.startField("key_value", 0);
      ColumnWriter key = create("key", 0, map.getKeys());
      ColumnWriter value = create("value", 1, map.getValues());
      for (int i = 0; i < map.getSize(); i++) {
        consumer.startGroup();
        key.write(consumer, i);
        value.write(consumer, i);
        consumer.endGroup();
      }
      consumer.endField("key_value", 0);
    }
    consumer.endGroup();
  }

  @FunctionalInterface
  private interface ValueWriter {
    void write(RecordConsumer consumer, int rowId);
  }

  /** Writes one field of a row; a null value leaves the field out. */
  static final class ColumnWriter {
    private final String name;
    private final int index;
    private final ColumnVector vector;
    private final ValueWriter value;

    private ColumnWriter(String name, int index, ColumnVector vector, ValueWriter value) {
      this.name = name;
      this.index = index;
      this.vector = vector;
      this.value = value;
    }

    void write(RecordConsumer consumer, int rowId) {
      if (vector.isNullAt(rowId)) {
        return;
      }
      consumer.startField(name, index);
      value.write(consumer, rowId);
      consumer.endField(name, index);
    }
  }
}
