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
package io.fleak.zephflow.lib.sql.exec.functions.maths;

import io.fleak.zephflow.lib.sql.exec.functions.BaseFunction;
import io.fleak.zephflow.lib.sql.exec.types.TypeCast;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import java.util.List;

public class Round extends BaseFunction {

  public static final String NAME = "round";
  private static TypeCast<Double> doubleTypeCast;
  private static TypeCast<Integer> integerTypeCast;

  public Round(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public Object apply(List<Object> args) {
    if (doubleTypeCast == null) doubleTypeCast = typeSystem.lookupTypeCast(Double.class);
    if (integerTypeCast == null) integerTypeCast = typeSystem.lookupTypeCast(Integer.class);

    var arg = args.get(0);
    if (arg == null) return null;

    var number = doubleTypeCast.cast(arg);
    if (args.size() > 1) {
      var decimals = integerTypeCast.cast(args.get(1));
      var factor = Math.pow(10, decimals);
      return Math.round(number * factor) / factor;
    } else {
      return Math.round(number);
    }
  }
}
