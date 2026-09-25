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
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.math.BigDecimal;
import java.math.BigInteger;
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
          throw new InvalidRecordException(
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
    return switch (type) {
      case ByteType t -> new ByteColumnVector(type, size);
      case ShortType t -> new ShortColumnVector(type, size);
      case IntegerType t -> new IntColumnVector(type, size);
      case DateType t -> new IntColumnVector(type, size);
      case LongType t -> new LongColumnVector(type, size);
      case TimestampType t -> new LongColumnVector(type, size);
      case TimestampNTZType t -> new LongColumnVector(type, size);
      case FloatType t -> new FloatColumnVector(type, size);
      case DoubleType t -> new DoubleColumnVector(type, size);
      case BooleanType t -> new BooleanColumnVector(type, size);
      case StringType t -> new StringColumnVector(type, size);
      case BinaryType t -> new BinaryColumnVector(type, size);
      case DecimalType t -> new DecimalColumnVector(type, size);
      default -> new ObjectColumnVector(type, size);
    };
  }

  /** Set null at the given row index */
  private static void setNull(ColumnVector vector, int rowId) {
    switch (vector) {
      case ByteColumnVector v -> v.setNull(rowId);
      case ShortColumnVector v -> v.setNull(rowId);
      case IntColumnVector v -> v.setNull(rowId);
      case LongColumnVector v -> v.setNull(rowId);
      case FloatColumnVector v -> v.setNull(rowId);
      case DoubleColumnVector v -> v.setNull(rowId);
      case BooleanColumnVector v -> v.setNull(rowId);
      case StringColumnVector v -> v.setNull(rowId);
      case BinaryColumnVector v -> v.setNull(rowId);
      case DecimalColumnVector v -> v.setNull(rowId);
      case ObjectColumnVector v -> v.set(rowId, null);
      default -> {}
    }
  }

  /** Set value at the given row index */
  private static void setValue(ColumnVector vector, int rowId, Object value, DataType dataType) {
    switch (vector) {
      case ByteColumnVector v -> v.set(rowId, numberValue(value, dataType).byteValue());
      case ShortColumnVector v -> v.set(rowId, numberValue(value, dataType).shortValue());
      case IntColumnVector v -> v.set(rowId, numberValue(value, dataType).intValue());
      case LongColumnVector v -> {
        if (value instanceof java.sql.Timestamp ts) {
          java.time.Instant inst = ts.toInstant();
          v.set(rowId, inst.getEpochSecond() * 1_000_000 + inst.getNano() / 1000);
        } else {
          v.set(rowId, numberValue(value, dataType).longValue());
        }
      }
      case FloatColumnVector v -> v.set(rowId, numberValue(value, dataType).floatValue());
      case DoubleColumnVector v -> v.set(rowId, numberValue(value, dataType).doubleValue());
      case BooleanColumnVector v -> {
        if (!(value instanceof Boolean bool)) {
          throw new InvalidRecordException("Expected Boolean value for " + dataType);
        }
        v.set(rowId, bool);
      }
      case StringColumnVector v -> v.set(rowId, value.toString());
      case BinaryColumnVector v -> {
        if (!(value instanceof byte[] bytes)) {
          throw new InvalidRecordException("Expected byte[] value for " + dataType);
        }
        v.set(rowId, bytes);
      }
      case DecimalColumnVector v -> v.set(rowId, convertToDecimal(value));
      case ObjectColumnVector v -> v.set(rowId, value);
      default -> {}
    }
  }

  private static Number numberValue(Object value, DataType type) {
    if (value instanceof Number number) {
      return number;
    }
    throw new InvalidRecordException("Expected Number value for " + type);
  }

  /** Infer schema from the first record in the data */
  public static StructType inferSchema(List<Map<String, Object>> data) {
    if (data.isEmpty()) {
      throw new IllegalArgumentException("Cannot infer schema from empty data");
    }

    Map<String, Object> firstRecord = data.getFirst();
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
    return switch (value) {
      case null -> StringType.STRING;
      case StringPrimitiveFleakData s -> StringType.STRING;
      case NumberPrimitiveFleakData numberData ->
          switch (numberData.getNumberType()) {
            case LONG -> LongType.LONG;
            case DOUBLE -> DoubleType.DOUBLE;
          };
      case BooleanPrimitiveFleakData b -> BooleanType.BOOLEAN;
      case ArrayFleakData arrayData -> {
        List<FleakData> arrayPayload = arrayData.getArrayPayload();
        yield !arrayPayload.isEmpty()
            ? new ArrayType(inferDataType(arrayPayload.getFirst()), true)
            : new ArrayType(StringType.STRING, true);
      }
      case RecordFleakData recordData -> {
        Map<String, FleakData> payload = recordData.getPayload();
        List<StructField> fields = new ArrayList<>();
        for (Map.Entry<String, FleakData> entry : payload.entrySet()) {
          fields.add(new StructField(entry.getKey(), inferDataType(entry.getValue()), true));
        }
        yield new StructType(fields);
      }
      case String s -> StringType.STRING;
      case Integer i -> IntegerType.INTEGER;
      case Long l -> LongType.LONG;
      case Double d -> DoubleType.DOUBLE;
      case Float f -> DoubleType.DOUBLE;
      case Boolean b -> BooleanType.BOOLEAN;
      case java.sql.Timestamp ts -> TimestampType.TIMESTAMP;
      case java.time.Instant inst -> TimestampType.TIMESTAMP;
      case BigDecimal bd -> new DecimalType(38, 18);
      case List<?> list -> {
        yield !list.isEmpty()
            ? new ArrayType(inferDataType(list.getFirst()), true)
            : new ArrayType(StringType.STRING, true);
      }
      case Map<?, ?> map -> {
        List<StructField> fields = new ArrayList<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          fields.add(
              new StructField(entry.getKey().toString(), inferDataType(entry.getValue()), true));
        }
        yield new StructType(fields);
      }
      default -> {
        log.debug("Unknown type for value {}, defaulting to STRING", value.getClass());
        yield StringType.STRING;
      }
    };
  }

  private static Object convertValueToSchemaType(
      Object value, DataType targetType, String fieldName) {
    if (value == null) {
      return null;
    }

    Object unwrappedValue = unwrapFleakData(value);

    return switch (targetType) {
      case StringType t -> unwrappedValue.toString();
      case ByteType t -> convertToByte(unwrappedValue);
      case ShortType t -> convertToShort(unwrappedValue);
      case IntegerType t -> convertToInt(unwrappedValue, fieldName);
      case DateType t -> convertToInt(unwrappedValue, fieldName);
      case LongType t -> convertToLong(unwrappedValue, fieldName);
      case TimestampType t -> convertToTimestamp(unwrappedValue, fieldName);
      case TimestampNTZType t -> convertToTimestamp(unwrappedValue, fieldName);
      case FloatType t -> convertToFloat(unwrappedValue);
      case DoubleType t -> convertToDouble(unwrappedValue, fieldName);
      case BooleanType t ->
          unwrappedValue instanceof Boolean b ? b : Boolean.parseBoolean(unwrappedValue.toString());
      case BinaryType t ->
          unwrappedValue instanceof byte[] bytes ? bytes : unwrappedValue.toString().getBytes();
      case DecimalType t -> convertToDecimal(unwrappedValue);
      case ArrayType arrayType -> {
        if (unwrappedValue instanceof List<?> list) {
          List<Object> convertedList = new ArrayList<>();
          for (Object element : list) {
            convertedList.add(
                convertValueToSchemaType(
                    element, arrayType.getElementType(), fieldName + "[element]"));
          }
          yield convertedList;
        }
        throw new InvalidRecordException(
            String.format(
                "Expected array for field '%s' but got %s",
                fieldName, unwrappedValue.getClass().getSimpleName()));
      }
      case MapType t -> {
        if (unwrappedValue instanceof Map<?, ?>) {
          yield unwrappedValue;
        }
        throw new InvalidRecordException(
            String.format(
                "Expected map for field '%s' but got %s",
                fieldName, unwrappedValue.getClass().getSimpleName()));
      }
      case StructType t -> {
        if (unwrappedValue instanceof Map) {
          yield unwrappedValue;
        }
        throw new InvalidRecordException(
            String.format(
                "Expected map/struct for field '%s' but got %s",
                fieldName, unwrappedValue.getClass().getSimpleName()));
      }
      default -> {
        log.debug("Using unwrapped value for field {} with type {}", fieldName, targetType);
        yield unwrappedValue;
      }
    };
  }

  private static byte convertToByte(Object value) {
    if (value instanceof Number number) return number.byteValue();
    try {
      return Byte.parseByte(value.toString());
    } catch (NumberFormatException e) {
      throw new InvalidRecordException("Cannot convert value to byte", e);
    }
  }

  private static short convertToShort(Object value) {
    if (value instanceof Number number) return number.shortValue();
    try {
      return Short.parseShort(value.toString());
    } catch (NumberFormatException e) {
      throw new InvalidRecordException("Cannot convert value to short", e);
    }
  }

  private static float convertToFloat(Object value) {
    if (value instanceof Number number) return number.floatValue();
    try {
      return Float.parseFloat(value.toString());
    } catch (NumberFormatException e) {
      throw new InvalidRecordException("Cannot convert value to float", e);
    }
  }

  private static Object convertToInt(Object unwrappedValue, String fieldName) {
    if (unwrappedValue instanceof Number n) return n.intValue();
    try {
      return Integer.parseInt(unwrappedValue.toString());
    } catch (NumberFormatException e) {
      throw new InvalidRecordException(
          String.format(
              "Cannot convert value '%s' to integer for field '%s'", unwrappedValue, fieldName),
          e);
    }
  }

  private static Object convertToLong(Object unwrappedValue, String fieldName) {
    if (unwrappedValue instanceof Number n) return n.longValue();
    try {
      return Long.parseLong(unwrappedValue.toString());
    } catch (NumberFormatException e) {
      throw new InvalidRecordException(
          String.format(
              "Cannot convert value '%s' to long for field '%s'", unwrappedValue, fieldName),
          e);
    }
  }

  private static Object convertToTimestamp(Object unwrappedValue, String fieldName) {
    return switch (unwrappedValue) {
      case java.sql.Timestamp ts -> ts;
      case java.time.Instant inst -> java.sql.Timestamp.from(inst);
      case Long l -> new java.sql.Timestamp(l);
      default -> {
        try {
          yield new java.sql.Timestamp(Long.parseLong(unwrappedValue.toString()));
        } catch (NumberFormatException e) {
          throw new InvalidRecordException(
              String.format(
                  "Cannot convert value '%s' to timestamp for field '%s'",
                  unwrappedValue, fieldName),
              e);
        }
      }
    };
  }

  private static Object convertToDouble(Object unwrappedValue, String fieldName) {
    if (unwrappedValue instanceof Number n) return n.doubleValue();
    try {
      return Double.parseDouble(unwrappedValue.toString());
    } catch (NumberFormatException e) {
      throw new InvalidRecordException(
          String.format(
              "Cannot convert value '%s' to double for field '%s'", unwrappedValue, fieldName),
          e);
    }
  }

  private static BigDecimal convertToDecimal(Object unwrappedValue) {
    try {
      return switch (unwrappedValue) {
        case BigDecimal bd -> bd;
        case Number n -> BigDecimal.valueOf(n.doubleValue());
        default -> new BigDecimal(unwrappedValue.toString());
      };
    } catch (NumberFormatException e) {
      throw new InvalidRecordException("Cannot convert value to decimal", e);
    }
  }

  private static Object unwrapFleakData(Object value) {
    if (value instanceof FleakData fd) {
      return fd.unwrap();
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
    private final int fixedWidthBytes;

    DecimalColumnVector(DataType type, int size) {
      this.type = type;
      this.fixedWidthBytes =
          type instanceof DecimalType decimalType && decimalType.getPrecision() > 18
              ? (BigInteger.TEN.pow(decimalType.getPrecision()).subtract(BigInteger.ONE).bitLength()
                      + 8)
                  / 8
              : 0;
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
      BigDecimal value = data[rowId];
      if (value != null && type instanceof DecimalType decimal && decimal.getPrecision() <= 18) {
        try {
          value.movePointRight(decimal.getScale());
        } catch (ArithmeticException failure) {
          throw new InvalidRecordException("Decimal value cannot be encoded for " + type, failure);
        }
      }
      if (value != null
          && fixedWidthBytes > 0
          && value.unscaledValue().toByteArray().length > fixedWidthBytes) {
        throw new InvalidRecordException(
            "Decimal value exceeds the Parquet fixed width for " + type);
      }
      return value;
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
      return switch (data[rowId]) {
        case null -> 0;
        case Number n -> n.intValue();
        default -> Integer.parseInt(data[rowId].toString());
      };
    }

    @Override
    public long getLong(int rowId) {
      return switch (data[rowId]) {
        case null -> 0L;
        case Number n -> n.longValue();
        default -> Long.parseLong(data[rowId].toString());
      };
    }

    @Override
    public double getDouble(int rowId) {
      return switch (data[rowId]) {
        case null -> 0.0;
        case Number n -> n.doubleValue();
        default -> Double.parseDouble(data[rowId].toString());
      };
    }

    @Override
    public boolean getBoolean(int rowId) {
      return switch (data[rowId]) {
        case null -> false;
        case Boolean b -> b;
        default -> Boolean.parseBoolean(data[rowId].toString());
      };
    }

    @Override
    public ArrayValue getArray(int rowId) {
      if (!(type instanceof ArrayType arrayType)) {
        throw new UnsupportedOperationException("getArray() is only supported for array types");
      }
      return switch (data[rowId]) {
        case null -> null;
        case List<?> list -> {
          @SuppressWarnings("unchecked")
          List<Object> objectList = (List<Object>) list;
          yield new SimpleArrayValue(objectList, arrayType);
        }
        default ->
            throw new InvalidRecordException(
                "Expected List for array type but got: " + data[rowId].getClass());
      };
    }

    @Override
    public MapValue getMap(int rowId) {
      if (!(type instanceof MapType mapType)) {
        throw new UnsupportedOperationException("getMap() is only supported for map types");
      }
      return switch (data[rowId]) {
        case null -> null;
        case Map<?, ?> map -> new SimpleMapValue(map, mapType);
        default ->
            throw new InvalidRecordException(
                "Expected Map for map type but got: " + data[rowId].getClass());
      };
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
              if (!field.isNullable()) {
                throw new InvalidRecordException(
                    "Cannot write NULL to non-nullable struct field " + fieldName);
              }
              setNull(childVector, i);
            } else {
              setValue(childVector, i, fieldValue, fieldType);
            }
          } else {
            if (!field.isNullable()) {
              throw new InvalidRecordException(
                  "Expected Map for non-nullable struct field " + fieldName);
            }
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
                if (!arrayType.containsNull()) {
                  throw new InvalidRecordException(
                      "Cannot write NULL to non-nullable array element");
                }
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

  private static final class SimpleMapValue implements MapValue {
    private final ColumnVector keys;
    private final ColumnVector values;

    SimpleMapValue(Map<?, ?> map, MapType type) {
      if (!(type.getKeyType() instanceof StringType)) {
        throw new UnsupportedOperationException("Only STRING map keys are supported");
      }
      List<Map.Entry<?, ?>> entries = new ArrayList<>(map.entrySet());
      keys = allocateVector(type.getKeyType(), entries.size());
      values = allocateVector(type.getValueType(), entries.size());
      for (int i = 0; i < entries.size(); i++) {
        Map.Entry<?, ?> entry = entries.get(i);
        if (!(entry.getKey() instanceof String key)) {
          throw new InvalidRecordException("Map keys must be non-null strings");
        }
        setValue(keys, i, key, type.getKeyType());
        Object value =
            convertValueToSchemaType(entry.getValue(), type.getValueType(), "[map value]");
        if (value == null) {
          if (!type.isValueContainsNull()) {
            throw new InvalidRecordException("Cannot write NULL to non-nullable map value");
          }
          setNull(values, i);
        } else {
          setValue(values, i, value, type.getValueType());
        }
      }
    }

    @Override
    public int getSize() {
      return keys.getSize();
    }

    @Override
    public ColumnVector getKeys() {
      return keys;
    }

    @Override
    public ColumnVector getValues() {
      return values;
    }
  }

  /** Simple implementation of ArrayValue for Delta Kernel API with lazy caching */
  private static final class SimpleArrayValue implements ArrayValue {
    private final List<Object> elements;
    private final DataType elementType;
    private final boolean containsNull;
    private ColumnVector cachedElements;

    SimpleArrayValue(List<Object> elements, ArrayType type) {
      this.elements = elements;
      this.elementType = type.getElementType();
      this.containsNull = type.containsNull();
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
          if (!containsNull) {
            throw new InvalidRecordException("Cannot write NULL to non-nullable array element");
          }
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
