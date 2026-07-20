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
import java.util.stream.Stream;
import org.graalvm.polyglot.*;

/*
arr_flatten:
Transforms an array containing nested arrays into an array by extracting all
immediate elements from the first level of nesting only. This function performs a shallow
flatten operation, not a deep recursive flatten.

Syntax:
```
arr_flatten(arr_of_arr)
```

Parameters:
- arr_of_arr: An array that may contain both simple elements and nested arrays at its first level.

Return Value:
- A new array with only one level of nesting removed. Deeper nested arrays remain intact.

Behavior:
- Extracts elements from first-level nested arrays only
- Does not recursively flatten deeper nested arrays
- Preserves the original order of elements
- Non-array elements are included as-is in the result

For example, given the input event:
```
{
  "f": [
    [1, [2, 3]],
    [4, 5],
    6
  ]
}
```

When applying:
```
arr_flatten($.f)
```

Results in:
```
[1, [2, 3], 4, 5, 6]
```
Note that the inner array [2, 3] remains intact as the function only flattens one level.
*/
class ArrFlattenFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("arr_flatten", 1, "array to flatten");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData fleakData = evaluatedArgs.getFirst();
    Preconditions.checkArgument(
        fleakData instanceof ArrayFleakData,
        "arr_flatten argument must be an array but found: %s",
        fleakData.unwrap());

    List<FleakData> arrPayload =
        fleakData.getArrayPayload().stream()
            .flatMap(l -> l instanceof ArrayFleakData ? l.getArrayPayload().stream() : Stream.of(l))
            .toList();

    return new ArrayFleakData(arrPayload);
  }
}
