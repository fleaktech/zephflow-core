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
import java.util.List;

/*
numCompareAnyFunction:
Compares a value against a number using one of lt/lte/gt/gte, coercing the value from a string when
needed. The value may be a scalar OR an array, in which case it returns true if any element
satisfies the comparison. Non-numeric / null values simply do not satisfy the comparison.

Syntax:
num_compare_any($.path.to.field, "lt", 1024)
*/
class NumCompareAnyFunction implements FeelFunction {

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required(
        "num_compare_any", 3, "value, operator (lt/lte/gt/gte), number");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData opFd = evaluatedArgs.get(1);
    Preconditions.checkArgument(
        opFd instanceof StringPrimitiveFleakData,
        "num_compare_any: operator must be a string: %s",
        opFd);
    FleakData operandFd = evaluatedArgs.get(2);
    Preconditions.checkArgument(
        operandFd instanceof NumberPrimitiveFleakData,
        "num_compare_any: operand must be a number: %s",
        operandFd);

    return new BooleanPrimitiveFleakData(
        satisfiesAny(evaluatedArgs.getFirst(), opFd.getStringValue(), operandFd.getNumberValue()));
  }

  private static boolean satisfiesAny(FleakData value, String op, double operand) {
    if (value == null) {
      return false;
    }
    if (value instanceof ArrayFleakData array) {
      for (FleakData element : array.getArrayPayload()) {
        if (satisfiesAny(element, op, operand)) {
          return true;
        }
      }
      return false;
    }
    Double number = toNumber(value);
    if (number == null) {
      return false;
    }
    return switch (op) {
      case "lt" -> number < operand;
      case "lte" -> number <= operand;
      case "gt" -> number > operand;
      case "gte" -> number >= operand;
      default -> throw new IllegalArgumentException("num_compare_any: unknown operator: " + op);
    };
  }

  private static Double toNumber(FleakData value) {
    if (value instanceof NumberPrimitiveFleakData n) {
      return n.getNumberValue();
    }
    if (value instanceof StringPrimitiveFleakData s) {
      try {
        return Double.parseDouble(s.getStringValue().trim());
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
