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
inFunction:
Check if a value exists in an array.

Syntax:
in(value, array)

Parameters:
- First argument: the value to search for
- Second argument: an array to search in

Returns: true if the value is found in the array, false otherwise.
Returns false if either argument is null.

Uses FleakData.equals() for comparison, so it works with primitives, strings, and objects.

Examples:
in(2, array(1, 2, 3))                    // true
in(5, array(1, 2, 3))                    // false
in("hello", array("hello", "world"))     // true
in(null, array(1, 2, 3))                 // false
in(1, null)                              // false
*/
class InFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("in", 2, "value and array");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData value = evaluatedArgs.getFirst();
    FleakData arrayArg = evaluatedArgs.get(1);

    if (value == null || arrayArg == null) {
      return FleakData.wrap(false);
    }

    Preconditions.checkArgument(
        arrayArg instanceof ArrayFleakData,
        "in: second argument must be an array but found: %s",
        arrayArg.unwrap());

    for (FleakData elem : arrayArg.getArrayPayload()) {
      if (value.equals(elem)) {
        return FleakData.wrap(true);
      }
    }
    return FleakData.wrap(false);
  }
}
