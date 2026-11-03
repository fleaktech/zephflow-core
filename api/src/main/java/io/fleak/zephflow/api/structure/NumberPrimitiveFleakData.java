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

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Represents a numeric primitive value in Fleak. JSON serialization will output just the number
 * value.
 */
@NoArgsConstructor
@JsonSerialize(using = FleakDataSerializer.class)
@JsonDeserialize(using = FleakDataDeserializer.class)
public class NumberPrimitiveFleakData implements PrimitiveFleakData {

  public static final double DOUBLE_COMPARE_DELTA = 0.000001d;

  @Getter private NumberType numberType;
  // The long itself for LONG, raw double bits for DOUBLE; a double holds only 53 integer bits
  private long bits;

  public NumberPrimitiveFleakData(double numberValue, NumberType numberType) {
    this.numberType = numberType;
    this.bits =
        numberType == NumberType.LONG
            ? (long) numberValue
            : Double.doubleToRawLongBits(numberValue);
  }

  public NumberPrimitiveFleakData(long longValue, NumberType numberType) {
    this.numberType = numberType;
    this.bits = numberType == NumberType.LONG ? longValue : Double.doubleToRawLongBits(longValue);
  }

  @Override
  public double getNumberValue() {
    return numberType == NumberType.LONG ? (double) bits : Double.longBitsToDouble(bits);
  }

  @Override
  public long getLongValue() {
    return numberType == NumberType.LONG ? bits : (long) Double.longBitsToDouble(bits);
  }

  public static boolean bothLong(FleakData l, FleakData r) {
    return l.getNumberType() == NumberType.LONG && r.getNumberType() == NumberType.LONG;
  }

  /**
   * Determines the resulting numeric type based on standard promotion rules. The type with the
   * highest precision is chosen. (DOUBLE > FLOAT > LONG > INT)
   *
   * @param type1 The first numeric type.
   * @param type2 The second numeric type.
   * @return The promoted numeric type.
   */
  public static NumberType getPromotedType(NumberType type1, NumberType type2) {
    if (type1 == NumberType.DOUBLE || type2 == NumberType.DOUBLE) {
      return NumberType.DOUBLE;
    }

    return NumberType.LONG;
  }

  @Override
  public boolean equals(Object that) {
    if (!(that instanceof NumberPrimitiveFleakData other)) {
      return false;
    }
    if (bothLong(this, other)) {
      return bits == other.bits;
    }
    return Math.abs(getNumberValue() - other.getNumberValue()) <= DOUBLE_COMPARE_DELTA;
  }

  int compareNumber(NumberPrimitiveFleakData other) {
    if (bothLong(this, other)) {
      return Long.compare(bits, other.bits);
    }
    return Double.compare(getNumberValue(), other.getNumberValue());
  }

  @Override
  public int hashCode() {
    return Double.hashCode(getNumberValue());
  }

  @Override
  public String toString() {
    return "NumberPrimitiveFleakData(numberValue="
        + getNumberValue()
        + ", numberType="
        + numberType
        + ")";
  }

  @Override
  @JsonValue
  public Number unwrap() {

    if (numberType == NumberType.LONG) {
      return bits;
    }
    return getNumberValue();
  }

  public enum NumberType {
    LONG,
    DOUBLE
  }
}
