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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/*
regexSearchAnyFunction:
Unanchored regex search against a value that may be a scalar OR an array. For an array it returns
true if the pattern is found anywhere within any element's string form.

Syntax:
regex_search_any($.path.to.field, "<regex_pattern>")
*/
class RegexSearchAnyFunction implements FeelFunction {
  private final Map<String, Pattern> patternCache = new ConcurrentHashMap<>();

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required(
        "regex_search_any", 2, "value (scalar or array) and regex pattern");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData patternFd = evaluatedArgs.get(1);
    Preconditions.checkArgument(
        patternFd instanceof StringPrimitiveFleakData,
        "regex_search_any: pattern must be a string: %s",
        patternFd);
    Pattern pattern = patternCache.computeIfAbsent(patternFd.getStringValue(), Pattern::compile);
    return new BooleanPrimitiveFleakData(
        RegexMatchAnyFunction.matches(evaluatedArgs.getFirst(), pattern, false));
  }
}
