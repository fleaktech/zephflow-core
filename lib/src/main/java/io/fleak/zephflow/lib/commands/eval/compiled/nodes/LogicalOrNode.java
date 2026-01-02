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

import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionNode;

/** Represents a short-circuit 'or' operation. */
public record LogicalOrNode(ExpressionNode left, ExpressionNode right) implements ExpressionNode {
  @Override
  public FleakData evaluate(EvalContext ctx) {
    FleakData leftVal = left.evaluate(ctx);
    if (leftVal != null && leftVal.isTrueValue()) {
      return new BooleanPrimitiveFleakData(true);
    }
    FleakData rightVal = right.evaluate(ctx);
    if (rightVal == null) {
      return new BooleanPrimitiveFleakData(false);
    }
    return new BooleanPrimitiveFleakData(rightVal.isTrueValue());
  }
}
