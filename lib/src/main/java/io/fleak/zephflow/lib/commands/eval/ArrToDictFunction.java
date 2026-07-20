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
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionNode;
import java.util.*;
import org.graalvm.polyglot.*;

class ArrToDictFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required(
        "arr_to_dict", 4, "array, variable name, key expression, and value expression");
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
    if (args.size() != 4) {
      throw new IllegalArgumentException(
          "arr_to_dict expects 4 arguments: array, variable name, key expression, and value expression");
    }

    FleakData arrayData = args.getFirst().evaluate(ctx);

    if (!(arrayData instanceof ArrayFleakData) && !(arrayData instanceof RecordFleakData)) {
      return null;
    }

    if (arrayData instanceof RecordFleakData) {
      arrayData = new ArrayFleakData(List.of(arrayData));
    }

    String elemVarName = lazyArgTexts.get(1);
    ExpressionNode keyNode = args.get(2);
    ExpressionNode valueNode = args.get(3);
    Map<String, FleakData> result = new LinkedHashMap<>();

    for (FleakData elem : arrayData.getArrayPayload()) {
      ctx.enterScope();
      try {
        ctx.setVariable(elemVarName, elem);
        FleakData keyResult = keyNode.evaluate(ctx);
        Preconditions.checkArgument(
            keyResult instanceof StringPrimitiveFleakData,
            "arr_to_dict: key expression must evaluate to a string but got: %s",
            keyResult == null ? "null" : keyResult.getClass().getSimpleName());
        String key = keyResult.getStringValue();
        FleakData value = valueNode.evaluate(ctx);
        result.put(key, value);
      } finally {
        ctx.exitScope();
      }
    }

    return new RecordFleakData(result);
  }
}
