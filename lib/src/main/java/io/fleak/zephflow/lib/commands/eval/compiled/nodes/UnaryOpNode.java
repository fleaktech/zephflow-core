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
import io.fleak.zephflow.lib.commands.eval.EvaluatorFuncs.UnaryEvaluatorFunc;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionNode;

/** Represents a unary operation (-, not). */
public record UnaryOpNode(ExpressionNode operand, UnaryEvaluatorFunc<FleakData> evaluator)
    implements ExpressionNode {
  @Override
  public FleakData evaluate(EvalContext ctx) {
    return evaluator.evaluate(operand.evaluate(ctx));
  }
}
