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
import java.util.*;
import lombok.extern.slf4j.Slf4j;

/**
 * Compares two Delta Kernel StructType schemas recursively.
 *
 * <p>Rules:
 *
 * <ul>
 *   <li>Type widening: INT→LONG OK, FLOAT→DOUBLE OK, reverse is ERROR
 *   <li>Decimal: expected(P1,S1) compatible with actual(P2,S2) if P1≤P2 and S1≤S2
 *   <li>Case-insensitive field matching
 *   <li>Missing field in actual: ERROR
 *   <li>Extra field in actual: OK (field will be null in data)
 *   <li>Nullability: expected nullable but actual NOT NULL → WARN (logged)
 * </ul>
 */
@Slf4j
public class SchemaComparator {

  public static List<String> compare(StructType expected, StructType actual) {
    return compare(expected, actual, "");
  }

  public static List<String> compare(StructType expected, StructType actual, String path) {
    List<String> errors = new ArrayList<>();
    Map<String, StructField> actualFields = buildFieldMap(actual);

    for (StructField expectedField : expected.fields()) {
      String fieldPath =
          path.isEmpty() ? expectedField.getName() : path + "." + expectedField.getName();
      StructField actualField = actualFields.get(expectedField.getName().toLowerCase());

      if (actualField == null) {
        errors.add("Missing field: " + fieldPath);
        continue;
      }

      List<String> typeErrors =
          compareTypes(expectedField.getDataType(), actualField.getDataType(), fieldPath);
      errors.addAll(typeErrors);

      if (expectedField.isNullable() && !actualField.isNullable()) {
        log.warn(
            "Field '{}' is nullable in expected schema but NOT NULL in actual - null values will be rejected",
            fieldPath);
      }
    }

    return errors;
  }

  private static Map<String, StructField> buildFieldMap(StructType structType) {
    Map<String, StructField> map = new HashMap<>();
    for (StructField field : structType.fields()) {
      map.put(field.getName().toLowerCase(), field);
    }
    return map;
  }

  private static List<String> compareTypes(DataType expected, DataType actual, String path) {
    List<String> errors = new ArrayList<>();

    if (expected instanceof StringType) {
      if (!(actual instanceof StringType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof IntegerType) {
      if (!(actual instanceof IntegerType
          || actual instanceof LongType
          || actual instanceof ShortType
          || actual instanceof ByteType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof LongType) {
      if (!(actual instanceof LongType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof ShortType) {
      if (!(actual instanceof ShortType
          || actual instanceof IntegerType
          || actual instanceof LongType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof ByteType) {
      if (!(actual instanceof ByteType
          || actual instanceof ShortType
          || actual instanceof IntegerType
          || actual instanceof LongType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof FloatType) {
      if (!(actual instanceof FloatType || actual instanceof DoubleType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof DoubleType) {
      if (!(actual instanceof DoubleType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof BooleanType) {
      if (!(actual instanceof BooleanType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof BinaryType) {
      if (!(actual instanceof BinaryType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof DateType) {
      if (!(actual instanceof DateType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof TimestampType) {
      if (!(actual instanceof TimestampType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof TimestampNTZType) {
      if (!(actual instanceof TimestampNTZType)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      }
    } else if (expected instanceof DecimalType expectedDecimal) {
      if (!(actual instanceof DecimalType actualDecimal)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      } else {
        if (expectedDecimal.getPrecision() > actualDecimal.getPrecision()
            || expectedDecimal.getScale() > actualDecimal.getScale()) {
          errors.add(
              String.format(
                  "'%s': expected DECIMAL(%d,%d), actual DECIMAL(%d,%d) - precision/scale too small",
                  path,
                  expectedDecimal.getPrecision(),
                  expectedDecimal.getScale(),
                  actualDecimal.getPrecision(),
                  actualDecimal.getScale()));
        }
      }
    } else if (expected instanceof ArrayType expectedArray) {
      if (!(actual instanceof ArrayType actualArray)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      } else {
        errors.addAll(
            compareTypes(
                expectedArray.getElementType(), actualArray.getElementType(), path + "[]"));
      }
    } else if (expected instanceof MapType expectedMap) {
      if (!(actual instanceof MapType actualMap)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      } else {
        errors.addAll(
            compareTypes(expectedMap.getKeyType(), actualMap.getKeyType(), path + "[key]"));
        errors.addAll(
            compareTypes(expectedMap.getValueType(), actualMap.getValueType(), path + "[value]"));
      }
    } else if (expected instanceof StructType expectedStruct) {
      if (!(actual instanceof StructType actualStruct)) {
        errors.add(formatTypeMismatch(path, expected, actual));
      } else {
        errors.addAll(compare(expectedStruct, actualStruct, path));
      }
    } else {
      errors.add("'" + path + "': unsupported type " + expected.getClass().getSimpleName());
    }

    return errors;
  }

  private static String formatTypeMismatch(DataType expected, DataType actual) {
    return String.format("expected %s, actual %s", describeType(expected), describeType(actual));
  }

  private static String formatTypeMismatch(String path, DataType expected, DataType actual) {
    return String.format(
        "'%s': expected %s, actual %s", path, describeType(expected), describeType(actual));
  }

  private static String describeType(DataType dataType) {
    if (dataType instanceof DecimalType dt) {
      return "DECIMAL(" + dt.getPrecision() + "," + dt.getScale() + ")";
    }
    if (dataType instanceof ArrayType at) {
      return "ARRAY<" + describeType(at.getElementType()) + ">";
    }
    if (dataType instanceof MapType mt) {
      return "MAP<" + describeType(mt.getKeyType()) + "," + describeType(mt.getValueType()) + ">";
    }
    if (dataType instanceof StructType st) {
      StringBuilder sb = new StringBuilder("STRUCT<");
      boolean first = true;
      for (StructField field : st.fields()) {
        if (!first) sb.append(",");
        sb.append(field.getName()).append(":").append(describeType(field.getDataType()));
        first = false;
      }
      sb.append(">");
      return sb.toString();
    }
    return dataType.getClass().getSimpleName().replace("Type", "").toUpperCase();
  }
}
