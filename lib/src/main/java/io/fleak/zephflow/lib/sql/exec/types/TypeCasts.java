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

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class TypeCasts {
  private static final TypeCast<Integer> INT_TYPE_CAST = new TypeCast.IntegerTypeCast();
  private static final TypeCast<Short> SHORT_TYPE_CAST = new TypeCast.ShortTypeCast();

  private static final TypeCast<Long> LONG_TYPE_CAST = new TypeCast.LongTypeCast();
  private static final TypeCast<Double> DOUBLE_TYPE_CAST = new TypeCast.DoubleTypeCast();
  private static final TypeCast<BigDecimal> BIG_DECIMAL_TYPE_CAST =
      new TypeCast.BigDecimalTypeCast();
  private static final TypeCast<Boolean> BOOLEAN_TYPE_CAST = new TypeCast.BooleanTypeCast();
  private static final TypeCast<String> STRING_TYPE_CAST = new TypeCast.StringTypeCast();
  private static final TypeCast<Object> NOOP_TYPE_CAST = new TypeCast.NoopTypeCast();

  private static final TypeCast<Map<?, ?>> MAP_TYPE_CAST = new TypeCast.MapTypeCast();
  private static final TypeCast<List<?>> LIST_TYPE_CAST = new TypeCast.ListTypeCast();
  private static final TypeCast<Iterable<?>> ITERABLE_TYPE_CAST = new TypeCast.IterableTypeCast();
  private static final TypeCast<JsonNode> JSON_TYPE_CAST = new TypeCast.JsonTypeCast();

  public static TypeCast<Long> longTypeCast() {
    return LONG_TYPE_CAST;
  }

  public static TypeCast<Integer> intTypeCast() {
    return INT_TYPE_CAST;
  }

  public static TypeCast<Short> shortTypeCast() {
    return SHORT_TYPE_CAST;
  }

  public static TypeCast<Double> doubleTypeCast() {
    return DOUBLE_TYPE_CAST;
  }

  public static TypeCast<BigDecimal> bigDecimalTypeCast() {
    return BIG_DECIMAL_TYPE_CAST;
  }

  public static TypeCast<Boolean> booleanTypeCast() {
    return BOOLEAN_TYPE_CAST;
  }

  public static TypeCast<String> stringTypeCast() {
    return STRING_TYPE_CAST;
  }

  public static TypeCast<Object> noopTypeCast() {
    return NOOP_TYPE_CAST;
  }

  public static TypeCast<Map<?, ?>> mapTypeCast() {
    return MAP_TYPE_CAST;
  }

  public static TypeCast<List<?>> listTypeCast() {
    return LIST_TYPE_CAST;
  }

  public static TypeCast<Iterable<?>> iterableCast() {
    return ITERABLE_TYPE_CAST;
  }

  public static TypeCast<JsonNode> jsonTypeCast() {
    return JSON_TYPE_CAST;
  }
}
