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

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.types.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class DeltaLakeDataConverterTest {

  @Test
  void testIntegerParseFailure() {
    StructType schema = new StructType(List.of(new StructField("age", IntegerType.INTEGER, false)));

    List<Map<String, Object>> data = List.of(Map.of("age", "invalid"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals(
        "Cannot convert value 'invalid' to integer for field 'age'", exception.getMessage());
  }

  @Test
  void testLongParseFailure() {
    StructType schema = new StructType(List.of(new StructField("id", LongType.LONG, false)));

    List<Map<String, Object>> data = List.of(Map.of("id", "not_a_number"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals(
        "Cannot convert value 'not_a_number' to long for field 'id'", exception.getMessage());
  }

  @Test
  void testDoubleParseFailure() {
    StructType schema = new StructType(List.of(new StructField("price", DoubleType.DOUBLE, false)));

    List<Map<String, Object>> data = List.of(Map.of("price", "abc"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals("Cannot convert value 'abc' to double for field 'price'", exception.getMessage());
  }

  @Test
  void testTimestampParseFailure() {
    StructType schema =
        new StructType(List.of(new StructField("created_at", TimestampType.TIMESTAMP, false)));

    List<Map<String, Object>> data = List.of(Map.of("created_at", "invalid_timestamp"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals(
        "Cannot convert value 'invalid_timestamp' to timestamp for field 'created_at'",
        exception.getMessage());
  }

  @Test
  void testArrayTypeMismatch() {
    StructType schema =
        new StructType(
            List.of(new StructField("tags", new ArrayType(StringType.STRING, true), false)));

    List<Map<String, Object>> data = List.of(Map.of("tags", "not_an_array"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals("Expected array for field 'tags' but got String", exception.getMessage());
  }

  @Test
  void testStructTypeMismatch() {
    StructType innerStruct =
        new StructType(List.of(new StructField("city", StringType.STRING, true)));
    StructType schema = new StructType(List.of(new StructField("address", innerStruct, false)));

    List<Map<String, Object>> data = List.of(Map.of("address", "not_a_map"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals("Expected map/struct for field 'address' but got String", exception.getMessage());
  }

  @Test
  void testSimpleColumnVectorValidationFailure() {
    StructType schema =
        new StructType(List.of(new StructField("count", IntegerType.INTEGER, false)));

    List<Map<String, Object>> data = List.of(Map.of("count", "not_an_integer"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals(
        "Cannot convert value 'not_an_integer' to integer for field 'count'",
        exception.getMessage());
  }

  @Test
  void testSuccessfulConversion() {
    StructType schema =
        new StructType(
            List.of(
                new StructField("name", StringType.STRING, false),
                new StructField("age", IntegerType.INTEGER, false)));

    List<Map<String, Object>> data =
        List.of(Map.of("name", "Alice", "age", 30), Map.of("name", "Bob", "age", 25));

    assertDoesNotThrow(
        () -> {
          var batch = DeltaLakeDataConverter.convertToColumnarBatch(data, schema);
          assertTrue(batch.hasNext());
          var filteredBatch = batch.next();
          assertEquals(2, filteredBatch.getData().getSize());
        });
  }
}
