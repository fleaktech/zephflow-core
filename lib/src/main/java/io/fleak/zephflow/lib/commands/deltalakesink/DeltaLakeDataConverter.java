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
package io.fleak.zephflow.lib.commands.deltalakesink;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

/**
 * Utility class for converting Map-based data to Delta Lake ColumnarBatch format. Uses optimized
 * type-specific ColumnVector implementations with primitive arrays to minimize boxing overhead and
 * GC pressure.
 */
@Slf4j
public class DeltaLakeDataConverter {

  /** Convert a list of Map data to FilteredColumnarBatch iterator using single-pass allocation */
  public static CloseableIterator<FilteredColumnarBatch> convertToColumnarBatch(
      List<Map<String, Object>> data, StructType schema) {
    if (data.isEmpty()) {
      return new EmptyCloseableIterator<>();
    }
    return new SingleElementIterator<>(convertSingleBatch(data, schema));
  }

  /** Convert a list of Map data to a single FilteredColumnarBatch */
  public static FilteredColumnarBatch convertSingleBatch(
      List<Map<String, Object>> data, StructType schema) {
    int batchSize = data.size();
    log.debug("Converting {} records to ColumnarBatch with schema: {}", batchSize, schema);

    Map<String, ColumnVector> columnVectors = new HashMap<>();

    for (StructField field : schema.fields()) {
      String fieldName = field.getName();
      DataType dataType = field.getDataType();

      ColumnVector vector = allocateVector(dataType, batchSize);

      for (int i = 0; i < batchSize; i++) {
        Object value = data.get(i).get(fieldName);

        if (value == null && !field.isNullable()) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot write NULL to non-nullable field '%s'. "
                      + "Please ensure the input data contains this required field. "
                      + "Available fields in input: %s",
                  fieldName, String.join(", ", data.get(i).keySet())));
        }

        if (value == null) {
          setNull(vector, i);
          continue;
        }

        Object converted = convertValueToSchemaType(value, dataType, fieldName);
        setValue(vector, i, converted, dataType);
      }
      columnVectors.put(fieldName, vector);
    }

    ColumnarBatch columnarBatch = new SimpleColumnarBatch(schema, columnVectors, batchSize);
    ColumnVector selectionVector = new AllTrueColumnVector(batchSize);
    return new FilteredColumnarBatch(columnarBatch, java.util.Optional.of(selectionVector));
  }

  /** Allocate the appropriate ColumnVector type based on DataType */
  private static ColumnVector allocateVector(DataType type, int size) {
    if (type instanceof ByteType) {
      return new ByteColumnVector(type, size);
    } else if (type instanceof ShortType) {
      return new ShortColumnVector(type, size);
    } else if (type instanceof IntegerType || type instanceof DateType) {
      return new IntColumnVector(type, size);
    } else if (type instanceof LongType
        || type instanceof TimestampType
        || type instanceof TimestampNTZType) {
      return new LongColumnVector(type, size);
    } else if (type instanceof FloatType) {
      return new FloatColumnVector(type, size);
    } else if (type instanceof DoubleType) {
      return new DoubleColumnVector(type, size);
    } else if (type instanceof BooleanType) {
      return new BooleanColumnVector(type, size);
    } else if (type instanceof StringType) {
      return new StringColumnVector(type, size);
    } else if (type instanceof BinaryType) {
      return new BinaryColumnVector(type, size);
    } else if (type instanceof DecimalType) {
      return new DecimalColumnVector(type, size);
    } else {
      // Arrays, Structs, Maps
      return new ObjectColumnVector(type, size);
    }
  }

  /** Set null at the given row index */
  private static void setNull(ColumnVector vector, int rowId) {
    if (vector instanceof ByteColumnVector v) v.setNull(rowId);
    else if (vector instanceof ShortColumnVector v) v.setNull(rowId);
    else if (vector instanceof IntColumnVector v) v.setNull(rowId);
    else if (vector instanceof LongColumnVector v) v.setNull(rowId);
    else if (vector instanceof FloatColumnVector v) v.setNull(rowId);
    else if (vector instanceof DoubleColumnVector v) v.setNull(rowId);
    else if (vector instanceof BooleanColumnVector v) v.setNull(rowId);
    else if (vector instanceof StringColumnVector v) v.setNull(rowId);
    else if (vector instanceof BinaryColumnVector v) v.setNull(rowId);
    else if (vector instanceof DecimalColumnVector v) v.setNull(rowId);
    else if (vector instanceof ObjectColumnVector v) v.set(rowId, null);
  }

  /** Set value at the given row index */
  private static void setValue(ColumnVector vector, int rowId, Object value, DataType dataType) {
    if (vector instanceof ByteColumnVector v) {
      v.set(rowId, ((Number) value).byteValue());
    } else if (vector instanceof ShortColumnVector v) {
      v.set(rowId, ((Number) value).shortValue());
    } else if (vector instanceof IntColumnVector v) {
      v.set(rowId, ((Number) value).intValue());
    } else if (vector instanceof LongColumnVector v) {
      if (value instanceof java.sql.Timestamp ts) {
        java.time.Instant inst = ts.toInstant();
        v.set(rowId, inst.getEpochSecond() * 1_000_000 + inst.getNano() / 1000);
      } else {
        v.set(rowId, ((Number) value).longValue());
      }
    } else if (vector instanceof FloatColumnVector v) {
      v.set(rowId, ((Number) value).floatValue());
    } else if (vector instanceof DoubleColumnVector v) {
      v.set(rowId, ((Number) value).doubleValue());
    } else if (vector instanceof BooleanColumnVector v) {
      v.set(rowId, (Boolean) value);
    } else if (vector instanceof StringColumnVector v) {
      v.set(rowId, value.toString());
    } else if (vector instanceof BinaryColumnVector v) {
      v.set(rowId, (byte[]) value);
    } else if (vector instanceof DecimalColumnVector v) {
      if (value instanceof BigDecimal bd) {
        v.set(rowId, bd);
      } else if (value instanceof Number n) {
        v.set(rowId, BigDecimal.valueOf(n.doubleValue()));
      } else {
        v.set(rowId, new BigDecimal(value.toString()));
      }
    } else if (vector instanceof ObjectColumnVector v) {
      v.set(rowId, value);
    }
  }

  /** Infer schema from the first record in the data */
  public static StructType inferSchema(List<Map<String, Object>> data) {
    if (data.isEmpty()) {
      throw new IllegalArgumentException("Cannot infer schema from empty data");
    }

    Map<String, Object> firstRecord = data.get(0);
    List<StructField> fields = new ArrayList<>();

    for (Map.Entry<String, Object> entry : firstRecord.entrySet()) {
      String fieldName = entry.getKey();
      Object value = entry.getValue();
      DataType dataType = inferDataType(value);
      fields.add(new StructField(fieldName, dataType, true));
    }

    return new StructType(fields);
  }

  private static DataType inferDataType(Object value) {
    if (value == null) {
      return StringType.STRING;
    }

    // Handle FleakData types first
    if (value instanceof StringPrimitiveFleakData) {
      return StringType.STRING;
    } else if (value instanceof NumberPrimitiveFleakData numberData) {
      return switch (numberData.getNumberType()) {
        case LONG -> LongType.LONG;
        case DOUBLE -> DoubleType.DOUBLE;
      };
    } else if (value instanceof BooleanPrimitiveFleakData) {
      return BooleanType.BOOLEAN;
    } else if (value instanceof ArrayFleakData arrayData) {
      List<FleakData> arrayPayload = arrayData.getArrayPayload();
      if (!arrayPayload.isEmpty()) {
        DataType elementType = inferDataType(arrayPayload.get(0));
        return new ArrayType(elementType, true);
      } else {
        return new ArrayType(StringType.STRING, true);
      }
    } else if (value instanceof RecordFleakData recordData) {
      Map<String, FleakData> payload = recordData.getPayload();
      List<StructField> fields = new ArrayList<>();
      for (Map.Entry<String, FleakData> entry : payload.entrySet()) {
        String fieldName = entry.getKey();
        DataType fieldType = inferDataType(entry.getValue());
        fields.add(new StructField(fieldName, fieldType, true));
      }
      return new StructType(fields);
    }

    // Handle regular Java types
    else if (value instanceof String) {
      return StringType.STRING;
    } else if (value instanceof Integer) {
      return IntegerType.INTEGER;
    } else if (value instanceof Long) {
      return LongType.LONG;
    } else if (value instanceof Double || value instanceof Float) {
      return DoubleType.DOUBLE;
    } else if (value instanceof Boolean) {
      return BooleanType.BOOLEAN;
    } else if (value instanceof java.sql.Timestamp || value instanceof java.time.Instant) {
      return TimestampType.TIMESTAMP;
    } else if (value instanceof BigDecimal) {
      return new DecimalType(38, 18);
    } else if (value instanceof List<?> list) {
      if (!list.isEmpty()) {
        DataType elementType = inferDataType(list.get(0));
        return new ArrayType(elementType, true);
      } else {
        return new ArrayType(StringType.STRING, true);
      }
    } else if (value instanceof Map<?, ?> map) {
      List<StructField> fields = new ArrayList<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        String fieldName = entry.getKey().toString();
        DataType fieldType = inferDataType(entry.getValue());
        fields.add(new StructField(fieldName, fieldType, true));
      }
      return new StructType(fields);
    } else {
      log.debug("Unknown type for value {}, defaulting to STRING", value.getClass());
      return StringType.STRING;
    }
  }

  private static Object convertValueToSchemaType(
      Object value, DataType targetType, String fieldName) {
    if (value == null) {
      return null;
    }

    Object unwrappedValue = unwrapFleakData(value);

    if (targetType instanceof StringType) {
      return unwrappedValue.toString();
    } else if (targetType instanceof ByteType) {
      if (unwrappedValue instanceof Number) {
        return ((Number) unwrappedValue).byteValue();
      }
      return Byte.parseByte(unwrappedValue.toString());
    } else if (targetType instanceof ShortType) {
      if (unwrappedValue instanceof Number) {
        return ((Number) unwrappedValue).shortValue();
      }
      return Short.parseShort(unwrappedValue.toString());
    } else if (targetType instanceof IntegerType || targetType instanceof DateType) {
      if (unwrappedValue instanceof Number) {
        return ((Number) unwrappedValue).intValue();
      }
      try {
        return Integer.parseInt(unwrappedValue.toString());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert value '%s' to integer for field '%s'", unwrappedValue, fieldName),
            e);
      }
    } else if (targetType instanceof LongType) {
      if (unwrappedValue instanceof Number) {
        return ((Number) unwrappedValue).longValue();
      }
      try {
        return Long.parseLong(unwrappedValue.toString());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert value '%s' to long for field '%s'", unwrappedValue, fieldName),
            e);
      }
    } else if (targetType instanceof TimestampType || targetType instanceof TimestampNTZType) {
      if (unwrappedValue instanceof java.sql.Timestamp) {
        return unwrappedValue;
      } else if (unwrappedValue instanceof java.time.Instant) {
        return java.sql.Timestamp.from((java.time.Instant) unwrappedValue);
      } else if (unwrappedValue instanceof Long) {
        return new java.sql.Timestamp((Long) unwrappedValue);
      } else {
        try {
          long timestamp = Long.parseLong(unwrappedValue.toString());
          return new java.sql.Timestamp(timestamp);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot convert value '%s' to timestamp for field '%s'",
                  unwrappedValue, fieldName),
              e);
        }
      }
    } else if (targetType instanceof FloatType) {
      if (unwrappedValue instanceof Number) {
        return ((Number) unwrappedValue).floatValue();
      }
      return Float.parseFloat(unwrappedValue.toString());
    } else if (targetType instanceof DoubleType) {
      if (unwrappedValue instanceof Number) {
        return ((Number) unwrappedValue).doubleValue();
      }
      try {
        return Double.parseDouble(unwrappedValue.toString());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot convert value '%s' to double for field '%s'", unwrappedValue, fieldName),
            e);
      }
    } else if (targetType instanceof BooleanType) {
      if (unwrappedValue instanceof Boolean) {
        return unwrappedValue;
      } else {
        return Boolean.parseBoolean(unwrappedValue.toString());
      }
    } else if (targetType instanceof BinaryType) {
      if (unwrappedValue instanceof byte[]) {
        return unwrappedValue;
      }
      return unwrappedValue.toString().getBytes();
    } else if (targetType instanceof DecimalType) {
      if (unwrappedValue instanceof BigDecimal) {
        return unwrappedValue;
      } else if (unwrappedValue instanceof Number) {
        return BigDecimal.valueOf(((Number) unwrappedValue).doubleValue());
      }
      return new BigDecimal(unwrappedValue.toString());
    } else if (targetType instanceof ArrayType arrayType) {
      if (unwrappedValue instanceof List<?> list) {
        List<Object> convertedList = new ArrayList<>();
        for (Object element : list) {
          Object convertedElement =
              convertValueToSchemaType(
                  element, arrayType.getElementType(), fieldName + "[element]");
          convertedList.add(convertedElement);
        }
        return convertedList;
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Expected array for field '%s' but got %s",
                fieldName, unwrappedValue.getClass().getSimpleName()));
      }
    } else if (targetType instanceof StructType) {
      if (unwrappedValue instanceof Map) {
        return unwrappedValue;
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Expected map/struct for field '%s' but got %s",
                fieldName, unwrappedValue.getClass().getSimpleName()));
      }
    }

    log.debug("Using unwrapped value for field {} with type {}", fieldName, targetType);
    return unwrappedValue;
  }

  private static Object unwrapFleakData(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof FleakData) {
      return ((FleakData) value).unwrap();
    }
    return value;
  }

  // ==================== Optimized ColumnVector Implementations ====================

  /** Optimized boolean column vector for selection vectors - always returns true */
  static final class AllTrueColumnVector implements ColumnVector {
    private final int size;

    AllTrueColumnVector(int size) {
      this.size = size;
    }

    @Override
    public DataType getDataType() {
      return BooleanType.BOOLEAN;
    }

    @Override
    public int getSize() {
      return size;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return false;
    }

    @Override
    public boolean getBoolean(int rowId) {
      return true;
    }

    @Override
    public void close() {}
  }

  /** Byte column vector using primitive byte[] */
  static final class ByteColumnVector implements ColumnVector {
    private final byte[] data;
    private final boolean[] nulls;
    private final DataType type;

    ByteColumnVector(DataType type, int size) {
      this.type = type;
      this.data = new byte[size];
      this.nulls = new boolean[size];
    }

    void set(int rowId, byte value) {
      data[rowId] = value;
    }

    void setNull(int rowId) {
      nulls[rowId] = true;
    }

    @Override
    public DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return data.length;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return nulls[rowId];
    }

    @Override
    public byte getByte(int rowId) {
      return data[rowId];
    }

    @Override
    public short getShort(int rowId) {
      return data[rowId];
    }

    @Override
    public int getInt(int rowId) {
      return data[rowId];
    }

    @Override
    public long getLong(int rowId) {
      return data[rowId];
    }

    @Override
    public double getDouble(int rowId) {
      return data[rowId];
    }

    @Override
    public String getString(int rowId) {
      return isNullAt(rowId) ? null : String.valueOf(data[rowId]);
    }

    @Override
    public void close() {}
  }

  /** Short column vector using primitive short[] */
  static final class ShortColumnVector implements ColumnVector {
    private final short[] data;
    private final boolean[] nulls;
    private final DataType type;

    ShortColumnVector(DataType type, int size) {
      this.type = type;
      this.data = new short[size];
      this.nulls = new boolean[size];
    }

    void set(int rowId, short value) {
      data[rowId] = value;
    }

    void setNull(int rowId) {
      nulls[rowId] = true;
    }

    @Override
    public DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return data.length;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return nulls[rowId];
    }

    @Override
    public short getShort(int rowId) {
      return data[rowId];
    }

    @Override
    public int getInt(int rowId) {
      return data[rowId];
    }

    @Override
    public long getLong(int rowId) {
      return data[rowId];
    }

    @Override
    public double getDouble(int rowId) {
      return data[rowId];
    }

    @Override
    public String getString(int rowId) {
      return isNullAt(rowId) ? null : String.valueOf(data[rowId]);
    }

    @Override
    public void close() {}
  }

  /** Int column vector using primitive int[] - handles IntegerType and DateType */
  static final class IntColumnVector implements ColumnVector {
    private final int[] data;
    private final boolean[] nulls;
    private final DataType type;

    IntColumnVector(DataType type, int size) {
      this.type = type;
      this.data = new int[size];
      this.nulls = new boolean[size];
    }

    void set(int rowId, int value) {
      data[rowId] = value;
    }

    void setNull(int rowId) {
      nulls[rowId] = true;
    }

    @Override
    public DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return data.length;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return nulls[rowId];
    }

    @Override
    public int getInt(int rowId) {
      return data[rowId];
    }

    @Override
    public long getLong(int rowId) {
      return data[rowId];
    }

    @Override
    public double getDouble(int rowId) {
      return data[rowId];
    }

    @Override
    public String getString(int rowId) {
      return isNullAt(rowId) ? null : String.valueOf(data[rowId]);
    }

    @Override
    public void close() {}
  }

  /**
   * Long column vector using primitive long[] - handles LongType, TimestampType, TimestampNTZType
   */
  static final class LongColumnVector implements ColumnVector {
    private final long[] data;
    private final boolean[] nulls;
    private final DataType type;

    LongColumnVector(DataType type, int size) {
      this.type = type;
      this.data = new long[size];
      this.nulls = new boolean[size];
    }

    void set(int rowId, long value) {
      data[rowId] = value;
    }

    void setNull(int rowId) {
      nulls[rowId] = true;
    }

    @Override
    public DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return data.length;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return nulls[rowId];
    }

    @Override
    public long getLong(int rowId) {
      return data[rowId];
    }

    @Override
    public double getDouble(int rowId) {
      return data[rowId];
    }

    @Override
    public String getString(int rowId) {
      return isNullAt(rowId) ? null : String.valueOf(data[rowId]);
    }

    @Override
    public void close() {}
  }

  /** Float column vector using primitive float[] */
  static final class FloatColumnVector implements ColumnVector {
    private final float[] data;
    private final boolean[] nulls;
    private final DataType type;

    FloatColumnVector(DataType type, int size) {
      this.type = type;
      this.data = new float[size];
      this.nulls = new boolean[size];
    }

    void set(int rowId, float value) {
      data[rowId] = value;
    }

    void setNull(int rowId) {
      nulls[rowId] = true;
    }

    @Override
    public DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return data.length;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return nulls[rowId];
    }

    @Override
    public float getFloat(int rowId) {
      return data[rowId];
    }

    @Override
    public double getDouble(int rowId) {
      return data[rowId];
    }

    @Override
    public String getString(int rowId) {
      return isNullAt(rowId) ? null : String.valueOf(data[rowId]);
    }

    @Override
    public void close() {}
  }

  /** Double column vector using primitive double[] */
  static final class DoubleColumnVector implements ColumnVector {
    private final double[] data;
    private final boolean[] nulls;
    private final DataType type;

    DoubleColumnVector(DataType type, int size) {
      this.type = type;
      this.data = new double[size];
      this.nulls = new boolean[size];
    }

    void set(int rowId, double value) {
      data[rowId] = value;
    }

    void setNull(int rowId) {
      nulls[rowId] = true;
    }

    @Override
    public DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return data.length;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return nulls[rowId];
    }

    @Override
    public double getDouble(int rowId) {
      return data[rowId];
    }

    @Override
    public String getString(int rowId) {
      return isNullAt(rowId) ? null : String.valueOf(data[rowId]);
    }

    @Override
    public void close() {}
  }

  /** Boolean column vector using primitive boolean[] */
  static final class BooleanColumnVector implements ColumnVector {
    private final boolean[] data;
    private final boolean[] nulls;
    private final DataType type;

    BooleanColumnVector(DataType type, int size) {
      this.type = type;
      this.data = new boolean[size];
      this.nulls = new boolean[size];
    }

    void set(int rowId, boolean value) {
      data[rowId] = value;
    }

    void setNull(int rowId) {
      nulls[rowId] = true;
    }

    @Override
    public DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return data.length;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return nulls[rowId];
    }

    @Override
    public boolean getBoolean(int rowId) {
      return data[rowId];
    }

    @Override
    public String getString(int rowId) {
      return isNullAt(rowId) ? null : String.valueOf(data[rowId]);
    }

    @Override
    public void close() {}
  }

  /** String column vector using String[] for better locality than List<Object> */
  static final class StringColumnVector implements ColumnVector {
    private final String[] data;
    private final DataType type;

    StringColumnVector(DataType type, int size) {
      this.type = type;
      this.data = new String[size];
    }

    void set(int rowId, String value) {
      data[rowId] = value;
    }

    void setNull(int rowId) {
      data[rowId] = null;
    }

    @Override
    public DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return data.length;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return data[rowId] == null;
    }

    @Override
    public String getString(int rowId) {
      return data[rowId];
    }

    @Override
    public void close() {}
  }

  /** Binary column vector using byte[][] */
  static final class BinaryColumnVector implements ColumnVector {
    private final byte[][] data;
    private final DataType type;

    BinaryColumnVector(DataType type, int size) {
      this.type = type;
      this.data = new byte[size][];
    }

    void set(int rowId, byte[] value) {
      data[rowId] = value;
    }

    void setNull(int rowId) {
      data[rowId] = null;
    }

    @Override
    public DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return data.length;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return data[rowId] == null;
    }

    @Override
    public byte[] getBinary(int rowId) {
      return data[rowId];
    }

    @Override
    public void close() {}
  }

  /** Decimal column vector using BigDecimal[] */
  static final class DecimalColumnVector implements ColumnVector {
    private final BigDecimal[] data;
    private final DataType type;

    DecimalColumnVector(DataType type, int size) {
      this.type = type;
      this.data = new BigDecimal[size];
    }

    void set(int rowId, BigDecimal value) {
      data[rowId] = value;
    }

    void setNull(int rowId) {
      data[rowId] = null;
    }

    @Override
    public DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return data.length;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return data[rowId] == null;
    }

    @Override
    public BigDecimal getDecimal(int rowId) {
      return data[rowId];
    }

    @Override
    public String getString(int rowId) {
      return isNullAt(rowId) ? null : data[rowId].toString();
    }

    @Override
    public void close() {}
  }

  /** Object column vector for complex types (ArrayType, StructType, MapType) */
  static final class ObjectColumnVector implements ColumnVector {
    private final Object[] data;
    private final DataType type;
    private final Map<Integer, ColumnVector> childCache = new HashMap<>();

    ObjectColumnVector(DataType type, int size) {
      this.type = type;
      this.data = new Object[size];
    }

    void set(int rowId, Object value) {
      data[rowId] = value;
    }

    @Override
    public DataType getDataType() {
      return type;
    }

    @Override
    public int getSize() {
      return data.length;
    }

    @Override
    public boolean isNullAt(int rowId) {
      return data[rowId] == null;
    }

    @Override
    public String getString(int rowId) {
      Object value = data[rowId];
      return value == null ? null : value.toString();
    }

    @Override
    public int getInt(int rowId) {
      Object value = data[rowId];
      if (value == null) return 0;
      if (value instanceof Number) return ((Number) value).intValue();
      return Integer.parseInt(value.toString());
    }

    @Override
    public long getLong(int rowId) {
      Object value = data[rowId];
      if (value == null) return 0L;
      if (value instanceof Number) return ((Number) value).longValue();
      return Long.parseLong(value.toString());
    }

    @Override
    public double getDouble(int rowId) {
      Object value = data[rowId];
      if (value == null) return 0.0;
      if (value instanceof Number) return ((Number) value).doubleValue();
      return Double.parseDouble(value.toString());
    }

    @Override
    public boolean getBoolean(int rowId) {
      Object value = data[rowId];
      if (value == null) return false;
      if (value instanceof Boolean) return (Boolean) value;
      return Boolean.parseBoolean(value.toString());
    }

    @Override
    public ArrayValue getArray(int rowId) {
      if (!(type instanceof ArrayType arrayType)) {
        throw new UnsupportedOperationException("getArray() is only supported for array types");
      }
      Object value = data[rowId];
      if (value == null) return null;
      if (value instanceof List) {
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) value;
        return new SimpleArrayValue(list, arrayType.getElementType());
      }
      throw new IllegalStateException("Expected List for array type but got: " + value.getClass());
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      ColumnVector cached = childCache.get(ordinal);
      if (cached != null) {
        return cached;
      }

      ColumnVector childVector;
      if (type instanceof StructType structType) {
        StructField field = structType.at(ordinal);
        String fieldName = field.getName();
        DataType fieldType = field.getDataType();

        childVector = allocateVector(fieldType, data.length);
        for (int i = 0; i < data.length; i++) {
          Object value = data[i];
          if (value == null) {
            setNull(childVector, i);
          } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            Object fieldValue = map.get(fieldName);
            if (fieldValue == null) {
              setNull(childVector, i);
            } else {
              setValue(childVector, i, fieldValue, fieldType);
            }
          } else {
            log.warn(
                "Expected Map for struct field {} at row {} but got {}",
                fieldName,
                i,
                value.getClass());
            setNull(childVector, i);
          }
        }
      } else if (type instanceof ArrayType arrayType) {
        if (ordinal != 0) {
          throw new IllegalArgumentException("Array types only have one child at ordinal 0");
        }
        DataType elementType = arrayType.getElementType();

        int totalElements = 0;
        for (Object value : data) {
          if (value instanceof List<?> list) {
            totalElements += list.size();
          }
        }

        childVector = allocateVector(elementType, totalElements);

        int idx = 0;
        for (Object value : data) {
          if (value instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            for (Object element : list) {
              if (element == null) {
                setNull(childVector, idx);
              } else {
                setValue(childVector, idx, element, elementType);
              }
              idx++;
            }
          }
        }
      } else {
        throw new UnsupportedOperationException(
            "getChild is only supported for struct and array types");
      }

      childCache.put(ordinal, childVector);
      return childVector;
    }

    @Override
    public void close() {}

    @Override
    public @NotNull String toString() {
      return "ObjectColumnVector{" + "dataType=" + type + ", size=" + data.length + '}';
    }
  }

  /** Simple implementation of ColumnarBatch */
  record SimpleColumnarBatch(
      StructType schema, Map<String, ColumnVector> columnVectors, int numRows)
      implements ColumnarBatch {

    public StructType getSchema() {
      return schema;
    }

    public int getSize() {
      return numRows;
    }

    public ColumnVector getColumnVector(int ordinal) {
      StructField field = schema.at(ordinal);
      return columnVectors.get(field.getName());
    }
  }

  /** Empty CloseableIterator implementation */
  private static class EmptyCloseableIterator<T> implements CloseableIterator<T> {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public T next() {
      throw new UnsupportedOperationException("Empty iterator has no elements");
    }

    @Override
    public void close() {}
  }

  /** Simple implementation of ArrayValue for Delta Kernel API with lazy caching */
  private static final class SimpleArrayValue implements ArrayValue {
    private final List<Object> elements;
    private final DataType elementType;
    private ColumnVector cachedElements;

    SimpleArrayValue(List<Object> elements, DataType elementType) {
      this.elements = elements;
      this.elementType = elementType;
    }

    @Override
    public int getSize() {
      return elements.size();
    }

    @Override
    public ColumnVector getElements() {
      if (cachedElements != null) {
        return cachedElements;
      }
      ColumnVector vector = allocateVector(elementType, elements.size());
      for (int i = 0; i < elements.size(); i++) {
        Object element = elements.get(i);
        if (element == null) {
          setNull(vector, i);
        } else {
          setValue(vector, i, element, elementType);
        }
      }
      cachedElements = vector;
      return vector;
    }
  }

  /** Single element CloseableIterator implementation */
  private static class SingleElementIterator<T> implements CloseableIterator<T> {
    private final T element;
    private boolean consumed = false;

    public SingleElementIterator(T element) {
      this.element = element;
    }

    @Override
    public boolean hasNext() {
      return !consumed;
    }

    @Override
    public T next() {
      if (consumed) {
        throw new UnsupportedOperationException("Iterator exhausted");
      }
      consumed = true;
      return element;
    }

    @Override
    public void close() {}
  }
}
