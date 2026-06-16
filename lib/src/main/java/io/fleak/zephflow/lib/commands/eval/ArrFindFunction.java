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
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionNode;
import java.util.*;
import org.graalvm.polyglot.*;

/*
arrFindFunction:
Find and return the first element in an array that satisfies a condition.
Returns null if no element matches.

Syntax:
```
arr_find($.path.to.array, variable_name, condition_expression)
```

where
- `$.path.to.array` points to the array field in the input event
- If `$.path.to.array` points to an object, treat that object as a single element array
- `variable_name` declares a variable that represents each array element
- `condition_expression` is a boolean expression evaluated for each element

For example:
Given the input event:
```
{
  "users": [
    { "name": "Alice", "id": "100" },
    { "name": "Bob", "id": "200" }
  ]
}
```

and the `arr_find` expression:
```
arr_find($.users, user, user.id == "100")
```

Results: `{ "name": "Alice", "id": "100" }`

With null safety:
```
dict(
  username=case(
    arr_find($.users, user, user.id == "100") != null =>
      arr_find($.users, user, user.id == "100").name,
    _ => null
  )
)
```
*/
class ArrFindFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("arr_find", 3, "array, variable name, and condition");
  }

  @Override
  public boolean isLazyEvaluation() {
    return true;
  }

  @Override
  public FleakData evaluateCompiled(
      EvalContext ctx,
      List<ExpressionNode> args,
      EvalExpressionParser.GenericFunctionCallContext originalCtx,
      List<String> lazyArgTexts) {
    if (args.size() != 3) {
      throw new IllegalArgumentException(
          "arr_find expects 3 arguments: array, variable name, and condition");
    }

    FleakData arrayData = args.getFirst().evaluate(ctx);

    if (!(arrayData instanceof ArrayFleakData) && !(arrayData instanceof RecordFleakData)) {
      return null;
    }

    if (arrayData instanceof RecordFleakData) {
      arrayData = new ArrayFleakData(List.of(arrayData));
    }

    String elemVarName = lazyArgTexts.get(1);
    ExpressionNode conditionNode = args.get(2);

    for (FleakData elem : arrayData.getArrayPayload()) {
      ctx.enterScope();
      try {
        ctx.setVariable(elemVarName, elem);
        FleakData conditionResult = conditionNode.evaluate(ctx);

        if (conditionResult instanceof BooleanPrimitiveFleakData && conditionResult.isTrueValue()) {
          return elem;
        }
      } finally {
        ctx.exitScope();
      }
    }

    return null;
  }
}
