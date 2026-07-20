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
import java.util.*;
import org.graalvm.polyglot.*;

/*
rangeFunction:
Generates an array of integer numbers based on the provided arguments.
The 'end' parameter in all variations is exclusive (the range goes up to,
but does not include, 'end').

Syntax and Behavior:

1. range(count)
   - Generates integers from 0 up to (but not including) 'count', with a step of 1.
   - Equivalent to range(0, count, 1).
   - Example: range(5) produces [0, 1, 2, 3, 4].
   - If 'count' is 0 or negative, an empty array is generated.
   - Example: range(-2) produces [].

2. range(start, end)
   - Generates integers from 'start' up to (but not including) 'end', with a step of 1.
   - Equivalent to range(start, end, 1).
   - Example: range(2, 5) produces [2, 3, 4].
   - If 'start' is greater than or equal to 'end', an empty array is generated.
   - Example: range(5, 2) produces [].

3. range(start, end, step)
   - Generates integers starting from 'start', incrementing by 'step', and stopping
     before reaching 'end'.
   - 'step' can be positive (to count up) or negative (to count down).
   - 'step' cannot be zero.
   - If 'step' is positive: numbers 'x' are generated as long as 'x < end'.
     Example: range(0, 10, 2) produces [0, 2, 4, 6, 8].
   - If 'step' is negative: numbers 'x' are generated as long as 'x > end'.
     Example: range(10, 0, -2) produces [10, 8, 6, 4, 2].
   - An empty array is generated if the conditions for generation are not met
     from the start (e.g., if 'start >= end' with a positive 'step', or
     if 'start <= end' with a negative 'step').
     Example: range(0, 5, -1) produces [].
     Example: range(5, 0, 1) produces [].
*/
class RangeFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.optional("range", 1, 3, "count, or start-end, or start-end-step");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    if (evaluatedArgs.isEmpty() || evaluatedArgs.size() > 3) {
      throw new IllegalArgumentException("range expects 1, 2, or 3 arguments");
    }

    int start = 0;
    int end;
    int step = 1;

    if (evaluatedArgs.size() == 1) {
      end = EvalContext.fleakDataToInt(evaluatedArgs.getFirst(), "count");
    } else if (evaluatedArgs.size() == 2) {
      start = EvalContext.fleakDataToInt(evaluatedArgs.getFirst(), "start");
      end = EvalContext.fleakDataToInt(evaluatedArgs.get(1), "end");
    } else {
      start = EvalContext.fleakDataToInt(evaluatedArgs.getFirst(), "start");
      end = EvalContext.fleakDataToInt(evaluatedArgs.get(1), "end");
      step = EvalContext.fleakDataToInt(evaluatedArgs.get(2), "step");
    }

    if (step == 0) {
      throw new IllegalArgumentException("range() step argument cannot be zero.");
    }

    long expectedSize;
    if (step > 0) {
      expectedSize = (end > start) ? ((long) (end - start - 1) / step + 1) : 0;
    } else {
      expectedSize = (start > end) ? ((long) (start - end - 1) / (-step) + 1) : 0;
    }

    int maxRangeSize = 1_000_000;
    if (expectedSize > maxRangeSize) {
      throw new IllegalArgumentException(
          String.format(
              "range() would generate %d elements, exceeding maximum of %d",
              expectedSize, maxRangeSize));
    }

    List<FleakData> resultNumbers = new ArrayList<>();
    if (step > 0) {
      for (long i = start; i < end; i += step) {
        resultNumbers.add(
            new NumberPrimitiveFleakData(i, NumberPrimitiveFleakData.NumberType.LONG));
      }
    } else {
      for (long i = start; i > end; i += step) {
        resultNumbers.add(
            new NumberPrimitiveFleakData(i, NumberPrimitiveFleakData.NumberType.LONG));
      }
    }

    return new ArrayFleakData(resultNumbers);
  }
}
