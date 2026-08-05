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

/*
concatFunction:
Join all arguments into a single string. Each argument is converted to string the same way as
to_str, and a null argument is treated as an empty string (never throws, unlike the `+` operator).
If every argument is null, returns null. Useful for rebuilding a value split across fields, e.g.:
```
concat($.file.parent_folder, "\\", $.file.name)
```
*/
class ConcatFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.variable("concat", 1, "one or more values joined into a string");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    StringBuilder sb = new StringBuilder();
    boolean anyNonNull = false;
    for (FleakData arg : evaluatedArgs) {
      if (arg == null) {
        continue;
      }
      anyNonNull = true;
      sb.append(Objects.toString(arg.unwrap()));
    }
    return anyNonNull ? FleakData.wrap(sb.toString()) : null;
  }
}
