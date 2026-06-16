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
import org.graalvm.polyglot.*;

/*
regexMatchFunction:
Tests if a string matches a regular expression pattern.

Syntax:
regex_match($.path.to.string.field, "<regex_pattern>")

Parameters:
- First argument: string to test
- Second argument: regex pattern (Java regex syntax)

Returns: true if string matches the entire pattern, false otherwise

Escape sequences:
Use double backslash (\\) to represent regex special characters:
- \\d  matches any digit
- \\s  matches any whitespace
- \\w  matches any word character
- \\.  matches a literal dot
- \\\\  matches a literal backslash

Examples:
regex_match($.num, "\\d+")                     // matches digits like "12345"
regex_match($.ver, "\\d+\\.\\d+\\.\\d+")       // matches version like "1.2.3"
regex_match($.text, "hello\\s+world")          // matches "hello world" with whitespace
regex_match($.email, "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
*/
class RegexMatchFunction implements FeelFunction {
  private final Map<String, Pattern> patternCache = new ConcurrentHashMap<>();

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("regex_match", 2, "input string and regex pattern");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData inputFd = evaluatedArgs.getFirst();
    if (inputFd == null) {
      return new BooleanPrimitiveFleakData(false);
    }
    Preconditions.checkArgument(
        inputFd instanceof StringPrimitiveFleakData,
        "regex_match: first argument must be a string: %s",
        inputFd);
    String input = inputFd.getStringValue();

    FleakData patternFd = evaluatedArgs.get(1);
    Preconditions.checkArgument(
        patternFd instanceof StringPrimitiveFleakData,
        "regex_match: pattern must be a string: %s",
        patternFd);
    String patternStr = patternFd.getStringValue();

    Pattern pattern = patternCache.computeIfAbsent(patternStr, Pattern::compile);
    boolean matches = pattern.matcher(input).matches();
    return new BooleanPrimitiveFleakData(matches);
  }
}
