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
package io.fleak.zephflow.lib.commands.sink;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SinkDataSizeEstimatorTest {

  @Test
  void nullIsZero() {
    assertEquals(0, SinkDataSizeEstimator.estimateValueBytes(null));
  }

  @Test
  void stringUsesUtf8Length() {
    assertEquals(5, SinkDataSizeEstimator.estimateValueBytes("hello"));
    // 2 chars, 6 UTF-8 bytes.
    assertEquals(6, SinkDataSizeEstimator.estimateValueBytes("日本"));
  }

  @Test
  void fixedWidthTypesUseBinaryWidths() {
    assertEquals(1, SinkDataSizeEstimator.estimateValueBytes(true));
    assertEquals(1, SinkDataSizeEstimator.estimateValueBytes((byte) 7));
    assertEquals(2, SinkDataSizeEstimator.estimateValueBytes((short) 7));
    assertEquals(4, SinkDataSizeEstimator.estimateValueBytes(7));
    assertEquals(4, SinkDataSizeEstimator.estimateValueBytes(7.0f));
    assertEquals(8, SinkDataSizeEstimator.estimateValueBytes(7L));
    assertEquals(8, SinkDataSizeEstimator.estimateValueBytes(7.0d));
  }

  @Test
  void byteArrayUsesItsLength() {
    assertEquals(3, SinkDataSizeEstimator.estimateValueBytes(new byte[] {1, 2, 3}));
  }

  @Test
  void unknownTypeFallsBackToStringForm() {
    // No binary width known, so measure "12.50".
    assertEquals(5, SinkDataSizeEstimator.estimateValueBytes(new BigDecimal("12.50")));
  }

  @Test
  void rowSumsValuesAndIgnoresColumnNames() {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("a_very_long_column_name", "ab"); // name must not be counted
    row.put("n", 1L);
    row.put("missing", null);

    assertEquals(2 + 8, SinkDataSizeEstimator.estimateRowBytes(row));
  }

  @Test
  void nullRowIsZero() {
    assertEquals(0, SinkDataSizeEstimator.estimateRowBytes(null));
  }
}
