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
import io.fleak.zephflow.lib.sql.ast.types.LikeStringPattern;
import io.fleak.zephflow.lib.sql.exec.functions.*;
import io.fleak.zephflow.lib.sql.exec.functions.maths.*;
import io.fleak.zephflow.lib.sql.exec.functions.strings.*;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class SQLTypeSystem extends TypeSystem {

  private final Map<String, Function<List<Object>, Object>> functions =
      Map.ofEntries(
          Map.entry(CastFn.NAME, new CastFn(this)),
          Map.entry(DateTruncFn.NAME, new DateTruncFn(this)),
          Map.entry(JsonGetToJson.NAME, new JsonGetToJson(this)),
          Map.entry(JsonGetToString.NAME, new JsonGetToString(this)),
          Map.entry(JsonArrayElements.NAME, new JsonArrayElements(this)),
          Map.entry(StringAgg.NAME, new StringAgg(this)),
          Map.entry(CountAgg.NAME, new CountAgg(this)),
          Map.entry(SumAgg.NAME, new SumAgg(this)),
          Map.entry(MaxAgg.NAME, new MaxAgg(this)),
          Map.entry(MinAgg.NAME, new MinAgg(this)),
          Map.entry(JsonAgg.NAME, new JsonAgg(this)),
          Map.entry(JsonBuildObject.NAME, new JsonBuildObject(this)),
          Map.entry(Concat.NAME, new Concat(this)),
          Map.entry(JsonArrayLength.NAME, new JsonArrayLength(this)),
          Map.entry(RegexpSplitToArray.NAME, new RegexpSplitToArray(this)),
          Map.entry(RegexpSplitToTable.NAME, new RegexpSplitToTable(this)),
          Map.entry(Replace.NAME, new Replace(this)),
          Map.entry(RTrim.NAME, new RTrim(this)),
          Map.entry(LTrim.NAME, new LTrim(this)),
          Map.entry(Substring.NAME, new Substring(this)),
          Map.entry(Trim.NAME, new Trim(this)),
          Map.entry(Upper.NAME, new Upper(this)),
          Map.entry(Lower.NAME, new Lower(this)),
          Map.entry(RegexpReplace.NAME, new RegexpReplace(this)),
          Map.entry(Length.NAME, new Length(this)),
          Map.entry(SplitPart.NAME, new SplitPart(this)),
          Map.entry(Position.NAME, new Position(this)),
          Map.entry(MD5.NAME, new MD5(this)),
          Map.entry(RegexpMatches.NAME, new RegexpMatches(this)),
          Map.entry(Format.NAME, new Format(this)),
          Map.entry(ConcatWs.NAME, new ConcatWs(this)),
          Map.entry(Left.NAME, new Left(this)),
          Map.entry(Right.NAME, new Right(this)),
          Map.entry(Reverse.NAME, new Reverse(this)),
          Map.entry(CharLength.NAME, new CharLength(this)),
          Map.entry(ToHex.NAME, new ToHex(this)),
          Map.entry(BTrim.NAME, new BTrim(this)),
          Map.entry(GrokFn.NAME, new GrokFn(this)),
          Map.entry(Abs.NAME, new Abs(this)),
          Map.entry(Ceil.NAME, new Ceil(this)),
          Map.entry(Floor.NAME, new Floor(this)),
          Map.entry(Round.NAME, new Round(this)),
          Map.entry(Sqrt.NAME, new Sqrt(this)),
          Map.entry(Power.NAME, new Power(this)),
          Map.entry(Mod.NAME, new Mod(this)),
          Map.entry(Random.NAME, new Random(this)),
          Map.entry(Trunc.NAME, new Trunc(this)),
          Map.entry(Sign.NAME, new Sign(this)),
          Map.entry(Gcd.NAME, new Gcd(this)),
          Map.entry(Lcm.NAME, new Lcm(this)),
          Map.entry(Exp.NAME, new Exp(this)),
          Map.entry(Log.NAME, new Log(this)),
          Map.entry(Ln.NAME, new Ln(this)),
          Map.entry(Log10.NAME, new Log10(this)),
          Map.entry(Log2.NAME, new Log2(this)),
          Map.entry(Degrees.NAME, new Degrees(this)),
          Map.entry(Radians.NAME, new Radians(this)),
          Map.entry(Pi.NAME, new Pi(this)),
          Map.entry(Sin.NAME, new Sin(this)),
          Map.entry(Cos.NAME, new Cos(this)),
          Map.entry(Tan.NAME, new Tan(this)));

  private final Map<Class<?>, Arithmetic<?>> classArithmeticMap =
      Map.of(
          Long.class, Arithmetics.longValue(),
          Integer.class, Arithmetics.longValue(),
          Short.class, Arithmetics.longValue(),
          Byte.class, Arithmetics.longValue(),
          Double.class, Arithmetics.doubleValue(),
          Float.class, Arithmetics.doubleValue(),
          BigDecimal.class, Arithmetics.bigDecimalValue(),
          String.class, Arithmetics.stringValue());

  private final Map<Class<?>, BooleanLogic<?>> classBooleanLogicMap =
      Map.of(
          Long.class, BooleanLogics.longValue(),
          Boolean.class, BooleanLogics.boolValue(),
          Integer.class, BooleanLogics.longValue(),
          Short.class, BooleanLogics.longValue(),
          Byte.class, BooleanLogics.longValue(),
          Float.class, BooleanLogics.doubleValue(),
          Double.class, BooleanLogics.doubleValue(),
          BigDecimal.class, BooleanLogics.bigDecimalValue(),
          String.class, BooleanLogics.stringValue(),
          LikeStringPattern.class, BooleanLogics.patternValue());

  private final Map<Class<?>, TypeCast<?>> classTypeCastMap =
      Map.ofEntries(
          Map.entry(Long.class, TypeCasts.longTypeCast()),
          Map.entry(Integer.class, TypeCasts.intTypeCast()),
          Map.entry(Short.class, TypeCasts.shortTypeCast()),
          Map.entry(Byte.class, TypeCasts.longTypeCast()),
          Map.entry(Float.class, TypeCasts.doubleTypeCast()),
          Map.entry(Double.class, TypeCasts.doubleTypeCast()),
          Map.entry(BigDecimal.class, TypeCasts.bigDecimalTypeCast()),
          Map.entry(Boolean.class, TypeCasts.booleanTypeCast()),
          Map.entry(String.class, TypeCasts.stringTypeCast()),
          Map.entry(Map.class, TypeCasts.mapTypeCast()),
          Map.entry(List.class, TypeCasts.listTypeCast()),
          Map.entry(Iterable.class, TypeCasts.iterableCast()),
          Map.entry(Object.class, TypeCasts.noopTypeCast()),
          Map.entry(JsonNode.class, TypeCasts.jsonTypeCast()));

  public <T> Arithmetic<T> lookupTypeArithmetic(Class<?> cls) {
    var armt = classArithmeticMap.get(cls);
    if (armt == null) throw new RuntimeException("arithmetic is not supported on type " + cls);

    return (Arithmetic<T>) armt;
  }

  public <T> BooleanLogic<T> lookupTypeBoolean(Class<?> cls) {
    var armt = classBooleanLogicMap.get(cls);
    if (armt == null)
      throw new RuntimeException("boolean/compare logic is not supported on type " + cls);

    return (BooleanLogic<T>) armt;
  }

  @Override
  public <T> TypeCast<T> lookupTypeCast(Class<T> clz) {
    var armt = classTypeCastMap.get(clz);
    if (armt == null) {
      throw new ClassCastException("type casting is not supported on type " + clz);
    }

    return (TypeCast<T>) armt;
  }

  @Override
  public Function<List<Object>, Object> lookupFunction(String name) {
    var fn = functions.get(name);
    if (fn == null) throw new RuntimeException(" the function " + name + " does not exist");

    return fn;
  }
}
