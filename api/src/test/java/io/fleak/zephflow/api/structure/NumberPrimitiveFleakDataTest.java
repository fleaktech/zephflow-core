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
package io.fleak.zephflow.api.structure;

import static io.fleak.zephflow.api.structure.NumberPrimitiveFleakData.NumberType.DOUBLE;
import static io.fleak.zephflow.api.structure.NumberPrimitiveFleakData.NumberType.LONG;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class NumberPrimitiveFleakDataTest {

  private static final long BIG = (1L << 53) + 1;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void longValueAbove2Pow53IsExact() {
    NumberPrimitiveFleakData n = new NumberPrimitiveFleakData(BIG, LONG);
    assertEquals(BIG, n.getLongValue());
    assertEquals(BIG, n.unwrap());
    assertEquals(LONG, n.getNumberType());
  }

  @Test
  void doubleConstructorsStoreTheDoubleValue() {
    NumberPrimitiveFleakData fromDouble = new NumberPrimitiveFleakData(2.5d, DOUBLE);
    assertEquals(2.5d, fromDouble.getNumberValue());
    assertEquals(2L, fromDouble.getLongValue());
    assertEquals(2.5d, fromDouble.unwrap());

    NumberPrimitiveFleakData fromLong = new NumberPrimitiveFleakData(7L, DOUBLE);
    assertEquals(7d, fromLong.getNumberValue());
    assertEquals(7L, fromLong.getLongValue());
  }

  @Test
  void doubleConstructorWithLongTypeTruncates() {
    NumberPrimitiveFleakData n = new NumberPrimitiveFleakData(3.9d, LONG);
    assertEquals(3L, n.getLongValue());
    assertEquals(3d, n.getNumberValue());
  }

  @Test
  void equalsComparesLongsExactlyAndMixedByDelta() {
    NumberPrimitiveFleakData big = new NumberPrimitiveFleakData(BIG, LONG);
    assertEquals(big, new NumberPrimitiveFleakData(BIG, LONG));
    assertNotEquals(big, new NumberPrimitiveFleakData(BIG - 1, LONG));
    assertEquals(new NumberPrimitiveFleakData(3L, LONG), new NumberPrimitiveFleakData(3d, DOUBLE));
    assertNotEquals(
        new NumberPrimitiveFleakData(3L, LONG), new NumberPrimitiveFleakData(3.1, DOUBLE));
    assertNotEquals(big, new StringPrimitiveFleakData("x"));
    assertEquals(
        new NumberPrimitiveFleakData(3L, LONG).hashCode(),
        new NumberPrimitiveFleakData(3d, DOUBLE).hashCode());
  }

  @Test
  void compareToOrdersLongsAbove2Pow53() {
    FleakData lower = new NumberPrimitiveFleakData(BIG - 1, LONG);
    FleakData higher = new NumberPrimitiveFleakData(BIG, LONG);
    assertTrue(lower.compareTo(higher) < 0);
    assertTrue(higher.compareTo(lower) > 0);
    assertTrue(new NumberPrimitiveFleakData(1.5, DOUBLE).compareTo(lower) < 0);
  }

  @Test
  void wrapKeepsIntegersExact() {
    assertEquals(BIG, FleakData.wrap(BIG).getLongValue());
    assertEquals(42L, FleakData.wrap(42).getLongValue());
  }

  @Test
  void getLongValueOnNonNumberThrows() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> new StringPrimitiveFleakData("x").getLongValue());
  }

  @Test
  void toStringShowsValueAndType() {
    assertEquals(
        "NumberPrimitiveFleakData(numberValue=5.0, numberType=LONG)",
        new NumberPrimitiveFleakData(5L, LONG).toString());
  }

  @Test
  void jsonRoundTripKeepsLongsExactAndDoublesAsDouble() throws Exception {
    FleakData big = MAPPER.readValue(Long.toString(BIG), FleakData.class);
    assertEquals(LONG, big.getNumberType());
    assertEquals(BIG, big.getLongValue());
    assertEquals(Long.toString(BIG), MAPPER.writeValueAsString(big));

    FleakData small = MAPPER.readValue("7", FleakData.class);
    assertEquals(LONG, small.getNumberType());
    assertEquals(7L, small.getLongValue());

    FleakData dbl = MAPPER.readValue("1.25", FleakData.class);
    assertEquals(DOUBLE, dbl.getNumberType());
    assertEquals(1.25d, dbl.getNumberValue());
  }
}
