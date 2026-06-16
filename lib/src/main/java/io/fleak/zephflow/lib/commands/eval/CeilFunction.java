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
package io.fleak.zephflow.lib.commands.eval;

import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import java.util.*;
import org.graalvm.polyglot.*;

/*
ceilFunction:
Round up a floating point number to the nearest integer.
Example: ceil(123.45) => 124, ceil(-123.45) => -123
*/
class CeilFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("ceil", 1, "number to round up");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData arg = evaluatedArgs.getFirst();
    if (!(arg instanceof NumberPrimitiveFleakData)) {
      throw new IllegalArgumentException(
          "ceil: argument must be a number but found: "
              + (arg != null ? arg.getClass().getSimpleName() : "null"));
    }

    double value = arg.getNumberValue();
    long ceilValue = (long) Math.ceil(value);
    return new NumberPrimitiveFleakData(ceilValue, NumberPrimitiveFleakData.NumberType.LONG);
  }
}
