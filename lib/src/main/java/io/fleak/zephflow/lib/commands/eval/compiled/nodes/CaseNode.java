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
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionNode;
import java.util.List;

/** Represents a case expression: case(cond1 => result1, cond2 => result2, ..., _ => default) */
public record CaseNode(List<WhenClause> whenClauses, ExpressionNode elseResult)
    implements ExpressionNode {

  public record WhenClause(ExpressionNode condition, ExpressionNode result) {}

  @Override
  public FleakData evaluate(EvalContext ctx) {
    for (WhenClause whenClause : whenClauses) {
      FleakData conditionResult = whenClause.condition().evaluate(ctx);
      if (conditionResult != null && conditionResult.isTrueValue()) {
        return whenClause.result().evaluate(ctx);
      }
    }
    return elseResult.evaluate(ctx);
  }
}
