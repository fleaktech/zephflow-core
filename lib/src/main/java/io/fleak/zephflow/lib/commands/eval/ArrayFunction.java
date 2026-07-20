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
arrayFunction:
Evaluate each argument and combine the result values into an array.
For example:
Given an input event:
```
{
    "f1": "a",
    "f2": "b"
}
```
The function call:
```
array($.f1, $.f2)
```
Results: `["a", "b"]`
*/
class ArrayFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.variable("array", 0, "zero or more expressions");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    if (evaluatedArgs.isEmpty()) {
      return new ArrayFleakData();
    }
    return new ArrayFleakData(new ArrayList<>(evaluatedArgs));
  }
}
