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
strContainsFunction:
test if the given string contains a substring.

Syntax:
str_contains(str, sub_str)

return `true` if `str` contains `sub_str`, otherwise `false`.
both arguments are required to be of string type
*/
class StrContainsFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("str_contains", 2, "string and substring");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData val1 = evaluatedArgs.getFirst();
    FleakData val2 = evaluatedArgs.get(1);
    if (val1 == null || val2 == null) {
      return FleakData.wrap(false);
    }
    boolean contains = val1.getStringValue().contains(val2.getStringValue());
    return FleakData.wrap(contains);
  }
}
