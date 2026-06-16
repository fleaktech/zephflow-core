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
import java.security.SecureRandom;
import java.util.*;
import org.graalvm.polyglot.*;

/*
randomLongFunction:
Returns a cryptographically secure random long number.
Example: random_long() => -8234782934729834729
*/
class RandomLongFunction implements FeelFunction {
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("random_long", 0, "no arguments");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    long randomValue = SECURE_RANDOM.nextLong();
    return new NumberPrimitiveFleakData(randomValue, NumberPrimitiveFleakData.NumberType.LONG);
  }
}
