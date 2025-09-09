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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a numeric primitive value in Fleak. JSON serialization will output just the number
 * value.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonSerialize(using = FleakDataSerializer.class)
@JsonDeserialize(using = FleakDataDeserializer.class)
public class NumberPrimitiveFleakData implements PrimitiveFleakData {

  public static final double DOUBLE_COMPARE_DELTA = 0.000001d;

  private double numberValue;
  private NumberType numberType;

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
    if (!(that instanceof NumberPrimitiveFleakData)) {
      return false;
    }
    return Math.abs(this.numberValue - ((NumberPrimitiveFleakData) that).numberValue)
        <= DOUBLE_COMPARE_DELTA;
  }

  @Override
  public int hashCode() {
    return Double.hashCode(this.numberValue);
  }

  @Override
  @JsonValue
  public Number unwrap() {

    if (numberType == NumberType.LONG) {
      return (long) numberValue;
    }
    return numberValue;
  }

  public enum NumberType {
    LONG,
    DOUBLE
  }
}
