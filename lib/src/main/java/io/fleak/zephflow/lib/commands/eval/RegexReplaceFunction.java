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

/*
regexReplaceFunction:
Replaces every match of a regular expression in a string.

Syntax:
regex_replace($.path.to.string.field, "<regex_pattern>", "<replacement>")

Parameters:
- First argument: string to rewrite
- Second argument: regex pattern (Java regex syntax)
- Third argument: replacement; $1, $2, ... insert capture groups, ${name} inserts a named group

Returns: the input with all matches replaced, or null if the input is null

Notes:
- All matches are replaced (global).
- Use inline flags for matching modes: (?i) case-insensitive, (?m) multiline, (?s) dot matches newline.
- To insert a literal $ or \ in the replacement, escape it as \\$ or \\\\.

Examples:
regex_replace($.msg, "\\s+", " ")                                   // collapse whitespace
regex_replace($.raw, "<Data Name='(\\w+)'>([^<]*)</Data>", "$1=$2") // XML element -> key=value
regex_replace($.raw, "(?s)<System>.*?</System>", "")                // strip a multi-line block
regex_replace($.ssn, "\\d{3}-\\d{2}-(\\d{4})", "***-**-$1")         // mask all but last 4 digits
*/
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
      // a malformed replacement: $3 when the pattern only has two groups, an unknown ${name},
      // or a trailing lone $
      throw new IllegalArgumentException(
          String.format(
              "regex_replace: replacement \"%s\" is invalid for pattern \"%s\": %s",
              replacement, pattern.pattern(), e.getMessage()),
          e);
    }
  }
}
