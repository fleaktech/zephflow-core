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
import java.util.*;
import org.graalvm.polyglot.*;

/*
parseIntFunction:
Parse a string into an integer. It's equivalent to Java `Long.parseLong()`

For example:
```
parse_int("3")
```
returns `3`.
*/
class ParseIntFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.optional("parse_int", 1, 2, "string to parse and optional radix");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData valueFd = evaluatedArgs.getFirst();
    Preconditions.checkArgument(
        valueFd instanceof StringPrimitiveFleakData,
        "parse_int: argument to be parsed is not a string: %s",
        valueFd);

    String intStr = valueFd.getStringValue();
    int radix = 10;

    if (evaluatedArgs.size() == 2) {
      FleakData radixFd = evaluatedArgs.get(1);
      radix = (int) radixFd.getNumberValue();
    }

    try {
      long value = Long.parseLong(intStr, radix);
      return new NumberPrimitiveFleakData(value, NumberPrimitiveFleakData.NumberType.LONG);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "parse_int: failed to parse int string: " + intStr + " with radix: " + radix);
    }
  }
}
