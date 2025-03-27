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

public class Arithmetics {
  private static final Arithmetic<Number> LONG = new Arithmetic.LongArithmetic();
  private static final Arithmetic<Number> DOUBLE = new Arithmetic.DoubleArithmetic();
  private static final Arithmetic<BigDecimal> BIG_DECIMAL = new Arithmetic.BigDecimalArithmetic();
  private static final Arithmetic<String> STRING = new Arithmetic.StringArithmetic();

  public static Arithmetic<Number> longValue() {
    return LONG;
  }

  public static Arithmetic<Number> doubleValue() {
    return DOUBLE;
  }

  public static Arithmetic<BigDecimal> bigDecimalValue() {
    return BIG_DECIMAL;
  }

  public static Arithmetic<String> stringValue() {
    return STRING;
  }
}
