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

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import java.text.NumberFormat;
import java.util.*;
import org.graalvm.polyglot.*;

/*
parseFloatFunction:
Parse a string into an float number. It's equivalent to Java `Double.parseDouble()`

For example:
```
parse_float("3.14")
```
returns `3.14`.
*/
class ParseFloatFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("parse_float", 1, "string to parse");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    return parseFloatValue(evaluatedArgs.getFirst());
  }

  private FleakData parseFloatValue(FleakData valueFd) {
    Preconditions.checkArgument(
        valueFd instanceof StringPrimitiveFleakData,
        "parse_float: argument to be parsed is not a string: %s",
        valueFd);

    String numberStr = valueFd.getStringValue();
    NumberFormat format = NumberFormat.getInstance(Locale.US);
    try {
      Number number = format.parse(numberStr);
      return new NumberPrimitiveFleakData(
          number.doubleValue(), NumberPrimitiveFleakData.NumberType.DOUBLE);
    } catch (Exception e) {
      throw new IllegalArgumentException("parse_float: failed to parse float string: " + numberStr);
    }
  }
}
