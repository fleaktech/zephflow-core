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
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.utils.CloseableIterator;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

/** Utility class for converting Map-based data to Delta Lake ColumnarBatch format */
@Slf4j
public class DeltaLakeDataConverter {

  /** Convert a list of Map data to FilteredColumnarBatch iterator */
  public static CloseableIterator<FilteredColumnarBatch> convertToColumnarBatch(
      List<Map<String, Object>> data, StructType schema) {

    if (data.isEmpty()) {
      return new EmptyCloseableIterator<>();
    }

    log.debug("Converting {} records to ColumnarBatch with schema: {}", data.size(), schema);

    // Create column vectors for each field in the schema
    Map<String, ColumnVector> columnVectors = new HashMap<>();

    for (StructField field : schema.fields()) {
      String fieldName = field.getName();
      DataType dataType = field.getDataType();

      // Extract values for this column from all records
      List<Object> columnValues = new ArrayList<>();
      for (Map<String, Object> record : data) {
        Object value = record.get(fieldName);
        columnValues.add(convertValueToSchemaType(value, dataType, fieldName));
      }

      // Create column vector for this field
      ColumnVector columnVector = createColumnVector(columnValues, dataType);
      columnVectors.put(fieldName, columnVector);
    }

    // Create ColumnarBatch
    FilteredColumnarBatch filteredBatch = getFilteredColumnarBatch(data, schema, columnVectors);

    // Return as single-element iterator
    return new SingleElementIterator<>(filteredBatch);
  }

  private static @NotNull FilteredColumnarBatch getFilteredColumnarBatch(
      List<Map<String, Object>> data, StructType schema, Map<String, ColumnVector> columnVectors) {
    ColumnarBatch columnarBatch = createColumnarBatch(schema, columnVectors, data.size());
    List<Object> allTrueValues = new ArrayList<>();
    for (int i = 0; i < data.size(); i++) {
      allTrueValues.add(Boolean.TRUE);
    }
    ColumnVector allRowsSelectedVector = new SimpleColumnVector(allTrueValues, BooleanType.BOOLEAN);
    return new FilteredColumnarBatch(columnarBatch, java.util.Optional.of(allRowsSelectedVector));
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

      fields.add(new StructField(fieldName, dataType, true)); // nullable = true
    }

    return new StructType(fields);
  }

  private static DataType inferDataType(Object value) {
    if (value == null) {
      return StringType.STRING; // Default to string for null values
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
        // Infer element type from first non-null element
        DataType elementType = inferDataType(arrayPayload.get(0));
        return new ArrayType(elementType, true); // nullable elements
      } else {
        return new ArrayType(StringType.STRING, true); // default to string array
      }
    } else if (value instanceof RecordFleakData recordData) {
      Map<String, FleakData> payload = recordData.getPayload();
      List<StructField> fields = new ArrayList<>();

      for (Map.Entry<String, FleakData> entry : payload.entrySet()) {
        String fieldName = entry.getKey();
        DataType fieldType = inferDataType(entry.getValue());
        fields.add(new StructField(fieldName, fieldType, true)); // nullable fields
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
    } else if (value instanceof List<?> list) {
      // Handle Java Lists - try to infer element type
      if (!list.isEmpty()) {
        DataType elementType = inferDataType(list.get(0));
        return new ArrayType(elementType, true);
      } else {
        return new ArrayType(StringType.STRING, true);
      }
    } else if (value instanceof Map<?, ?> map) {
      // Handle Java Maps as structs
      List<StructField> fields = new ArrayList<>();

      for (Map.Entry<?, ?> entry : map.entrySet()) {
        String fieldName = entry.getKey().toString();
        DataType fieldType = inferDataType(entry.getValue());
        fields.add(new StructField(fieldName, fieldType, true));
      }

      return new StructType(fields);
    } else {
      // For unknown types, convert to string
      log.debug("Unknown type for value {}, defaulting to STRING", value.getClass());
      return StringType.STRING;
    }
  }

  private static Object convertValueToSchemaType(
      Object value, DataType targetType, String fieldName) {
    if (value == null) {
      return null;
    }

    // First, unwrap FleakData objects to get their native values
    Object unwrappedValue = unwrapFleakData(value);

    // Convert unwrapped value to match the target data type
    if (targetType.equals(StringType.STRING)) {
      return unwrappedValue.toString();
    } else if (targetType.equals(IntegerType.INTEGER)) {
      if (unwrappedValue instanceof Number) {
        return ((Number) unwrappedValue).intValue();
      }
      try {
        return Integer.parseInt(unwrappedValue.toString());
      } catch (NumberFormatException e) {
        log.warn(
            "Could not convert value {} to integer for field {}, using null",
            unwrappedValue,
            fieldName);
        return null;
      }
    } else if (targetType.equals(LongType.LONG)) {
      if (unwrappedValue instanceof Number) {
        return ((Number) unwrappedValue).longValue();
      }
      try {
        return Long.parseLong(unwrappedValue.toString());
      } catch (NumberFormatException e) {
        log.warn(
            "Could not convert value {} to long for field {}, using null",
            unwrappedValue,
            fieldName);
        return null;
      }
    } else if (targetType.equals(DoubleType.DOUBLE)) {
      if (unwrappedValue instanceof Number) {
        return ((Number) unwrappedValue).doubleValue();
      }
      try {
        return Double.parseDouble(unwrappedValue.toString());
      } catch (NumberFormatException e) {
        log.warn(
            "Could not convert value {} to double for field {}, using null",
            unwrappedValue,
            fieldName);
        return null;
      }
    } else if (targetType.equals(BooleanType.BOOLEAN)) {
      if (unwrappedValue instanceof Boolean) {
        return unwrappedValue;
      } else {
        return Boolean.parseBoolean(unwrappedValue.toString());
      }
    } else if (targetType.equals(TimestampType.TIMESTAMP)) {
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
          log.warn(
              "Could not convert value {} to timestamp for field {}, using null",
              unwrappedValue,
              fieldName);
          return null;
        }
      }
    } else if (targetType instanceof ArrayType arrayType) {
      // Handle array types
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
        log.warn(
            "Expected array for field {} but got {}, using empty array",
            fieldName,
            unwrappedValue.getClass());
        return new ArrayList<>();
      }
    } else if (targetType instanceof StructType) {
      // Handle struct types
      if (unwrappedValue instanceof Map) {
        return unwrappedValue; // Maps are handled naturally by Delta Lake
      } else {
        log.warn(
            "Expected map/struct for field {} but got {}, using empty map",
            fieldName,
            unwrappedValue.getClass());
        return new HashMap<>();
      }
    }

    // For other types, use the unwrapped value
    log.debug("Using unwrapped value for field {} with type {}", fieldName, targetType);
    return unwrappedValue;
  }

  /**
   * Unwraps FleakData objects to their native Java values, recursively handling nested structures.
   */
  private static Object unwrapFleakData(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof FleakData) {
      return ((FleakData) value).unwrap();
    }

    // For non-FleakData objects, return as-is
    return value;
  }

  private static ColumnVector createColumnVector(List<Object> values, DataType dataType) {
    // This is a simplified implementation
    // In a real implementation, you would create appropriate column vectors
    // based on the Delta Kernel's ColumnVector implementations
    return new SimpleColumnVector(values, dataType);
  }

  private static ColumnarBatch createColumnarBatch(
      StructType schema, Map<String, ColumnVector> columnVectors, int numRows) {
    return new SimpleColumnarBatch(schema, columnVectors, numRows);
  }

  /**
   * Enhanced implementation of ColumnVector that properly handles different data types and null
   * values for use with Delta Kernel API
   */
  record SimpleColumnVector(List<Object> values, DataType dataType) implements ColumnVector {
    SimpleColumnVector(List<Object> values, DataType dataType) {
      this.values = new ArrayList<>(values); // Create defensive copy
      this.dataType = dataType;

      // Validate that all non-null values are compatible with the declared type
      validateValues();
    }

    private void validateValues() {
      for (int i = 0; i < values.size(); i++) {
        Object value = values.get(i);
        if (value != null && !isValueCompatibleWithType(value, dataType)) {
          log.warn(
              "Value at index {} ({}) is not compatible with type {}, setting to null",
              i,
              value,
              dataType);
          values.set(i, null);
        }
      }
    }

    private boolean isValueCompatibleWithType(Object value, DataType dataType) {
      if (dataType.equals(StringType.STRING)) {
        return true; // Any value can be converted to string
      } else if (dataType.equals(IntegerType.INTEGER)) {
        return value instanceof Integer;
      } else if (dataType.equals(LongType.LONG)) {
        return value instanceof Long;
      } else if (dataType.equals(DoubleType.DOUBLE)) {
        return value instanceof Double || value instanceof Float;
      } else if (dataType.equals(BooleanType.BOOLEAN)) {
        return value instanceof Boolean;
      } else if (dataType.equals(TimestampType.TIMESTAMP)) {
        return value instanceof Timestamp || value instanceof Instant;
      } else if (dataType instanceof ArrayType) {
        return value instanceof List;
      } else if (dataType instanceof StructType) {
        return value instanceof Map;
      }
      return true; // For other types, assume compatibility
    }

    @Override
    public DataType getDataType() {
      return dataType;
    }

    @Override
    public int getSize() {
      return values.size();
    }

    @Override
    public void close() {
      // No resources to close for this simple implementation
    }

    @Override
    public boolean isNullAt(int rowId) {
      if (rowId < 0 || rowId >= values.size()) {
        throw new IndexOutOfBoundsException(
            "Row index " + rowId + " out of bounds for size " + values.size());
      }
      return values.get(rowId) == null;
    }

    public Object get(int rowId) {
      if (rowId < 0 || rowId >= values.size()) {
        throw new IndexOutOfBoundsException(
            "Row index " + rowId + " out of bounds for size " + values.size());
      }
      return values.get(rowId);
    }

    // Additional methods for complex type support
    @Override
    public String getString(int rowId) {
      validateRowId(rowId);
      Object value = get(rowId);
      return value == null ? null : value.toString();
    }

    @Override
    public int getInt(int rowId) {
      validateRowId(rowId);
      Object value = get(rowId);
      if (value == null) return 0;
      if (value instanceof Number) {
        return ((Number) value).intValue();
      }
      try {
        return Integer.parseInt(value.toString());
      } catch (NumberFormatException e) {
        return 0;
      }
    }

    @Override
    public long getLong(int rowId) {
      validateRowId(rowId);
      Object value = get(rowId);
      if (value == null) return 0L;
      if (value instanceof Number) {
        return ((Number) value).longValue();
      }
      try {
        return Long.parseLong(value.toString());
      } catch (NumberFormatException e) {
        return 0L;
      }
    }

    @Override
    public double getDouble(int rowId) {
      validateRowId(rowId);
      Object value = get(rowId);
      if (value == null) return 0.0;
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      }
      try {
        return Double.parseDouble(value.toString());
      } catch (NumberFormatException e) {
        return 0.0;
      }
    }

    @Override
    public boolean getBoolean(int rowId) {
      validateRowId(rowId);
      Object value = get(rowId);
      if (value == null) return false;
      if (value instanceof Boolean) {
        return (Boolean) value;
      }
      return Boolean.parseBoolean(value.toString());
    }

    // Override additional methods that might be called by Parquet writer
    // to ensure they never return null Optional values
    @Override
    public @NotNull String toString() {
      return "SimpleColumnVector{" + "dataType=" + dataType + ", size=" + values.size() + '}';
    }

    // Ensure we handle bounds properly
    private void validateRowId(int rowId) {
      if (rowId < 0 || rowId >= values.size()) {
        throw new IndexOutOfBoundsException(
            "Row index " + rowId + " out of bounds for size " + values.size());
      }
    }

    @Override
    public ArrayValue getArray(int rowId) {
      validateRowId(rowId);
      if (!(dataType instanceof ArrayType arrayType)) {
        throw new UnsupportedOperationException("getArray() is only supported for array types");
      }

      Object value = get(rowId);
      if (value == null) {
        return null;
      }

      if (value instanceof List) {
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) value;

        // Create a simple ArrayValue implementation
        return new SimpleArrayValue(list, arrayType.getElementType());
      } else {
        throw new IllegalStateException(
            "Expected List for array type but got: " + value.getClass());
      }
    }

    @Override
    public ColumnVector getChild(int ordinal) {
      if (dataType instanceof StructType structType) {
        StructField field = structType.at(ordinal);
        String fieldName = field.getName();
        DataType fieldType = field.getDataType();

        // Extract child values for this field from all struct values
        List<Object> childValues = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
          Object value = values.get(i);
          if (value == null) {
            childValues.add(null);
          } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            Object fieldValue = map.get(fieldName);
            // Ensure we maintain the same row count even if field is missing
            childValues.add(fieldValue);
          } else {
            log.warn(
                "Expected Map for struct field {} at row {} but got {}",
                fieldName,
                i,
                value.getClass());
            childValues.add(null);
          }
        }

        // Ensure child vector has the same size as parent
        while (childValues.size() < values.size()) {
          childValues.add(null);
        }

        return new SimpleColumnVector(childValues, fieldType);
      } else if (dataType instanceof ArrayType arrayType) {
        // For arrays, ordinal should be 0 for the element vector
        if (ordinal != 0) {
          throw new IllegalArgumentException("Array types only have one child at ordinal 0");
        }

        DataType elementType = arrayType.getElementType();

        // Extract all elements from all arrays
        List<Object> allElements = new ArrayList<>();
        for (Object value : values) {
          if (value == null) {
            // For null arrays, we don't add any elements
            continue;
          }
          if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            allElements.addAll(list);
          } else {
            log.warn("Expected List for array field but got {}", value.getClass());
          }
        }

        return new SimpleColumnVector(allElements, elementType);
      } else {
        throw new UnsupportedOperationException(
            "getChild is only supported for struct and array types");
      }
    }
  }

  /** Simple implementation of ColumnarBatch for demonstration */
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
    public void close() {
      // Nothing to close
    }
  }

  /** Simple implementation of ArrayValue for Delta Kernel API */
  private record SimpleArrayValue(List<Object> elements, DataType elementType)
      implements ArrayValue {

    @Override
    public int getSize() {
      return elements.size();
    }

    @Override
    public ColumnVector getElements() {
      // Return a column vector containing all the array elements
      return new SimpleColumnVector(elements, elementType);
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
    public void close() {
      // Nothing to close
    }
  }
}
