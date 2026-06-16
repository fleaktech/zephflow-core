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
substrFunction:
Extract a substring from a string using SQL or Python style syntax.

Supports multiple overloads:
1. substr(string, start) - Extract from start position to end of string
   - Positive start: 0-based index from beginning
   - Negative start: Index from end (-1 is last character)

2. substr(string, start, length) - SQL style: Extract 'length' characters starting at 'start'
   - Positive start: 0-based index from beginning
   - Negative start: Index from end
   - length: Number of characters to extract

Edge cases:
- Start index out of bounds:
  * start > string length: Returns empty string ""
  * start < -string length: Treated as 0 (beginning of string)
- Length exceeds remaining characters: Returns substring up to end of string
- Invalid argument types (non-integer start/length): Throws error
- Null string input: Throws error
- Negative length: Throws error

Examples:
- substr("hello", 1) returns "ello" (from index 1 to end)
- substr("hello", -2) returns "lo" (last 2 characters)
- substr("hello", 1, 2) returns "el" (2 characters starting at index 1)
- substr("hello", -3, 2) returns "ll" (2 characters starting 3 from end)
- substr("hello", 10) returns "" (start beyond string length)
- substr("hello", -10) returns "hello" (negative start clamped to 0)
- substr("hello", 2, 100) returns "llo" (length truncated to available characters)
*/
class SubstrFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.optional(
        "substr", 2, 3, "string, start position, and optional length");
  }

  private String subStr(String str, int start, int length) {
    if (str == null) {
      throw new IllegalArgumentException("Input string cannot be null");
    }
    if (length < 0) {
      throw new IllegalArgumentException("Length cannot be negative");
    }

    int strLen = str.length();

    if (start < 0) {
      start = strLen + start;
      if (start < 0) {
        start = 0;
      }
    }

    if (start >= strLen) {
      return "";
    }

    int endPos;
    if (length == Integer.MAX_VALUE || start > Integer.MAX_VALUE - length) {
      endPos = strLen;
    } else {
      endPos = start + length;
      if (endPos > strLen) {
        endPos = strLen;
      }
    }

    return str.substring(start, endPos);
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    if (evaluatedArgs.size() < 2 || evaluatedArgs.size() > 3) {
      throw new IllegalArgumentException("substr expects 2 or 3 arguments");
    }

    FleakData strFd = evaluatedArgs.getFirst();
    if (strFd == null) {
      return null;
    }
    String str = strFd.getStringValue();

    int start = EvalContext.fleakDataToInt(evaluatedArgs.get(1), "start");
    int length = Integer.MAX_VALUE;

    if (evaluatedArgs.size() == 3) {
      length = EvalContext.fleakDataToInt(evaluatedArgs.get(2), "length");
    }

    String result = subStr(str, start, length);
    return FleakData.wrap(result);
  }
}
