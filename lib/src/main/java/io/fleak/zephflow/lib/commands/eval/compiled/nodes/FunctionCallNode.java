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
package io.fleak.zephflow.lib.commands.eval.compiled.nodes;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.FeelFunction;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionNode;
import java.util.List;
import java.util.stream.Collectors;

/** Represents a function call with compiled argument nodes. */
public record FunctionCallNode(
    String functionName,
    List<ExpressionNode> argNodes,
    FeelFunction function,
    EvalExpressionParser.GenericFunctionCallContext originalCtx,
    List<String> lazyArgTexts)
    implements ExpressionNode {

  @Override
  public FleakData evaluate(EvalContext ctx) {
    validateArgumentCount();

    if (function.isLazyEvaluation()) {
      return function.evaluateCompiled(ctx, argNodes, originalCtx, lazyArgTexts);
    }

    List<FleakData> evaluatedArgs =
        argNodes.stream().map(node -> node.evaluate(ctx)).collect(Collectors.toList());
    return function.evaluateCompiledEager(ctx, evaluatedArgs, originalCtx);
  }

  private void validateArgumentCount() {
    FeelFunction.FunctionSignature sig = function.getSignature();
    int argCount = argNodes.size();

    if (argCount < sig.minArgs()) {
      throw new IllegalArgumentException(
          String.format(
              "%s expects %s arguments but got %d",
              sig.functionName(), formatExpectedArgs(sig), argCount));
    }

    if (sig.maxArgs() >= 0 && argCount > sig.maxArgs()) {
      throw new IllegalArgumentException(
          String.format(
              "%s expects %s arguments but got %d",
              sig.functionName(), formatExpectedArgs(sig), argCount));
    }
  }

  private String formatExpectedArgs(FeelFunction.FunctionSignature sig) {
    if (sig.maxArgs() < 0) {
      return "at least " + sig.minArgs();
    } else if (sig.minArgs() == sig.maxArgs()) {
      return String.valueOf(sig.minArgs());
    } else {
      return sig.minArgs() + " to " + sig.maxArgs();
    }
  }
}
