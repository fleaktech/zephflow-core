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

class StrReplaceFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("str_replace", 3, "string, target, and replacement");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData stringData = evaluatedArgs.getFirst();
    FleakData targetData = evaluatedArgs.get(1);
    FleakData replacementData = evaluatedArgs.get(2);

    if (stringData == null) {
      return null;
    }

    Preconditions.checkArgument(
        targetData instanceof StringPrimitiveFleakData,
        "str_replace: second argument (target) must be a string but found: %s",
        targetData == null ? "null" : targetData.unwrap());

    Preconditions.checkArgument(
        replacementData instanceof StringPrimitiveFleakData,
        "str_replace: third argument (replacement) must be a string but found: %s",
        replacementData == null ? "null" : replacementData.unwrap());

    String input = stringData.getStringValue();
    String target = targetData.getStringValue();
    String replacement = replacementData.getStringValue();

    return new StringPrimitiveFleakData(input.replace(target, replacement));
  }
}
