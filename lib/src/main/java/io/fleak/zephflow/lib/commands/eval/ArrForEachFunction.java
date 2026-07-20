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
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.*;

/*
arrForEachFunction:
Apply the same logic for every element in a given array.
Returns a new array containing the transformed elements.
Syntax:
```
arr_foreach($.path.to.array, variable_name, expression)
```
where
- `$.path.to.array` points to the array field in the input event.
- If `$.path.to.array` points to an object, treat that object as a single element array
- declares a variable name that represents each array element
- the expression logic to be applied on every array element. use `variable_name` to refer to the array element

For example:
Given the input event:
```
{
  "integration": "snmp",
  "attachments": {
    "snmp_pdf": "s3://a.pdf",
    "f1": "s3://b.pdf"
  },
  "resp": {
    "Test1": [
      {
        "operation_system": "windows",
        "ipAddr": "1.2.3.4"
      },
      {
        "operation_system": "windows",
        "ipAddr": "1.2.3.5"
      }
    ]
  }
}

```

and the `arr_foreach` expression:
```
arr_foreach(
    $.resp.Test1,
    elem,
    dict(
        osVersion=elem.operation_system,
        source=$.integration,
        pdf_attachment=$.attachments.snmp_pdf,
        ip=elem.ipAddr
    )
)
```

Results:
```
[
    {
      "source": "snmp",
      "osVersion": "windows",
      "pdf_attachment": "s3://a.pdf",
      "ip": "1.2.3.4"
    },
    {
      "source": "snmp",
      "osVersion": "windows",
      "pdf_attachment": "s3://a.pdf",
      "ip": "1.2.3.5"
    }
]
```
*/
@Slf4j
class ArrForEachFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("arr_foreach", 3, "array, variable name, and expression");
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
          "arr_foreach expects 3 arguments: array, variable name, and expression");
    }

    FleakData arrayData = args.getFirst().evaluate(ctx);
    if (!(arrayData instanceof ArrayFleakData) && !(arrayData instanceof RecordFleakData)) {
      return null;
    }

    if (arrayData instanceof RecordFleakData) {
      arrayData = new ArrayFleakData(List.of(arrayData));
    }

    String elemVarName = lazyArgTexts.get(1);
    ExpressionNode expressionNode = args.get(2);

    List<FleakData> resultArray = new ArrayList<>();
    for (FleakData elem : arrayData.getArrayPayload()) {
      ctx.enterScope();
      try {
        ctx.setVariable(elemVarName, elem);
        FleakData resultElem = expressionNode.evaluate(ctx);
        resultArray.add(resultElem);
      } catch (Exception e) {
        log.error("arr_foreach: skipping failed element. Reason: {}", e.getMessage());
      } finally {
        ctx.exitScope();
      }
    }

    return new ArrayFleakData(resultArray);
  }
}
