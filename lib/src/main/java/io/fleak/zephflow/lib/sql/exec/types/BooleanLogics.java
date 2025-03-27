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
package io.fleak.zephflow.lib.sql.exec.types;

import java.math.BigDecimal;

public class BooleanLogics {

  public static BooleanLogic.DoubleBooleanLogic DOUBLE_BOOLEAN_LOGIC =
      new BooleanLogic.DoubleBooleanLogic();
  public static BooleanLogic.LongBooleanLogic LONG_BOOLEAN_LOGIC =
      new BooleanLogic.LongBooleanLogic();
  public static BooleanLogic.BoolBooleanLogic BOOL_BOOLEAN_LOGIC =
      new BooleanLogic.BoolBooleanLogic();
  public static BooleanLogic.BigDecimalBooleanLogic BIG_DECIMAL_BOOLEAN_LOGIC =
      new BooleanLogic.BigDecimalBooleanLogic();
  public static BooleanLogic.StringBooleanLogic STRING_BOOLEAN_LOGIC =
      new BooleanLogic.StringBooleanLogic();
  public static BooleanLogic.LikePatternBooleanLogic LIKE_BOOLEAN_LOGIC =
      new BooleanLogic.LikePatternBooleanLogic();

  public static BooleanLogic<Boolean> boolValue() {
    return BOOL_BOOLEAN_LOGIC;
  }

  public static BooleanLogic<Number> doubleValue() {
    return DOUBLE_BOOLEAN_LOGIC;
  }

  public static BooleanLogic<Number> longValue() {
    return LONG_BOOLEAN_LOGIC;
  }

  public static BooleanLogic<BigDecimal> bigDecimalValue() {
    return BIG_DECIMAL_BOOLEAN_LOGIC;
  }

  public static BooleanLogic<String> stringValue() {
    return STRING_BOOLEAN_LOGIC;
  }

  public static BooleanLogic<String> patternValue() {
    return LIKE_BOOLEAN_LOGIC;
  }
}
