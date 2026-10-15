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
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

class RegexReplaceFunction implements FeelFunction {
  private final Map<String, Pattern> patternCache = new ConcurrentHashMap<>();

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required(
        "regex_replace", 3, "input string, regex pattern, and replacement");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData inputFd = evaluatedArgs.getFirst();
    FleakData patternFd = evaluatedArgs.get(1);
    FleakData replacementFd = evaluatedArgs.get(2);

    if (inputFd == null) {
      return null;
    }

    Preconditions.checkArgument(
        inputFd instanceof StringPrimitiveFleakData,
        "regex_replace: first argument must be a string but found: %s",
        inputFd.unwrap());

    Preconditions.checkArgument(
        patternFd instanceof StringPrimitiveFleakData,
        "regex_replace: second argument (pattern) must be a string but found: %s",
        patternFd == null ? "null" : patternFd.unwrap());

    Preconditions.checkArgument(
        replacementFd instanceof StringPrimitiveFleakData,
        "regex_replace: third argument (replacement) must be a string but found: %s",
        replacementFd == null ? "null" : replacementFd.unwrap());

    String input = inputFd.getStringValue();
    String replacement = replacementFd.getStringValue();
    Pattern pattern = patternCache.computeIfAbsent(patternFd.getStringValue(), Pattern::compile);

    try {
      return new StringPrimitiveFleakData(pattern.matcher(input).replaceAll(replacement));
    } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format(
              "regex_replace: replacement \"%s\" is invalid for pattern \"%s\": %s",
              replacement, pattern.pattern(), e.getMessage()),
          e);
    }
  }
}
