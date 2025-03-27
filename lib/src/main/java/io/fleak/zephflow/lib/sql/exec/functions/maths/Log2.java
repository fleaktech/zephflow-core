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

public class Log2 extends BaseFunction {

  public static final String NAME = "log2";
  private static TypeCast<Double> doubleTypeCast;
  private static final double LOG2_BASE = Math.log(2);

  public Log2(TypeSystem typeSystem) {
    super(typeSystem, NAME);
  }

  @Override
  public Object apply(List<Object> args) {
    if (doubleTypeCast == null) doubleTypeCast = typeSystem.lookupTypeCast(Double.class);

    assertArgs(args, 1, "(number)");

    var arg = args.get(0);
    if (arg == null) return null;

    var number = doubleTypeCast.cast(arg);
    return Math.log(number) / LOG2_BASE;
  }
}
