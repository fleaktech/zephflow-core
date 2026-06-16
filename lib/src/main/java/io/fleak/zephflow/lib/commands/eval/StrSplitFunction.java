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

/*
strSplitFunction:
Split a string into an array of substrings based on a delimiter.

Syntax:
str_split(string, delimiter)

Parameters:
- First argument: string to split
- Second argument: delimiter (literal string, NOT regex)

Returns: array of substrings, preserving empty strings (including trailing)

Delimiter handling:
The delimiter is matched literally - special regex characters like . | * + [ have no special meaning.
Use standard string escape sequences for special characters:
- \"   matches a double quote
- \\   matches a backslash
- \n   matches a newline

Examples:
str_split("a,b,c", ",")           // ["a", "b", "c"]
str_split("a.b.c", ".")           // ["a", "b", "c"] - dot is literal
str_split("a|b|c", "|")           // ["a", "b", "c"] - pipe is literal
str_split("a,b,,", ",")           // ["a", "b", "", ""] - trailing empty strings preserved
str_split("a\"b\"c", "\"")        // ["a", "b", "c"] - split by double quote
*/
class StrSplitFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("str_split", 2, "string and delimiter");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData stringData = evaluatedArgs.getFirst();
    FleakData delimiterData = evaluatedArgs.get(1);

    if (stringData == null) {
      return null;
    }

    if (delimiterData == null) {
      return FleakData.wrap(List.of(stringData));
    }

    Preconditions.checkArgument(
        stringData instanceof StringPrimitiveFleakData,
        "str_split: first argument must be a string but found: %s",
        stringData.unwrap());

    Preconditions.checkArgument(
        delimiterData instanceof StringPrimitiveFleakData,
        "str_split: second argument (delimiter) must be a string but found: %s",
        delimiterData.unwrap());

    String inputString = stringData.getStringValue();
    String delimiter = delimiterData.getStringValue();

    if (inputString == null || inputString.isEmpty()) {
      return new ArrayFleakData(List.of());
    }

    List<FleakData> resultList = new ArrayList<>();
    int start = 0;
    int idx;
    while ((idx = inputString.indexOf(delimiter, start)) != -1) {
      resultList.add(new StringPrimitiveFleakData(inputString.substring(start, idx)));
      start = idx + delimiter.length();
    }
    resultList.add(new StringPrimitiveFleakData(inputString.substring(start)));

    return new ArrayFleakData(resultList);
  }
}
