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

/*
deepContainsFunction:
Recursively searches a value (record, array, or primitive) for a needle substring, matching
case-insensitively against the string form of every primitive found within.

Syntax:
deep_contains($, "powershell")

Parameters:
- First argument: the haystack — any value; typically the whole event root ($)
- Second argument: the needle (string)

Returns: true if the needle appears anywhere inside the haystack, false otherwise. A null
haystack yields false.
*/
class DeepContainsFunction implements FeelFunction {

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("deep_contains", 2, "haystack value and needle string");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData haystack = evaluatedArgs.getFirst();
    if (haystack == null) {
      return new BooleanPrimitiveFleakData(false);
    }
    FleakData needleFd = evaluatedArgs.get(1);
    Preconditions.checkArgument(
        needleFd instanceof StringPrimitiveFleakData,
        "deep_contains: needle must be a string: %s",
        needleFd);

    String needle = needleFd.getStringValue().toLowerCase();
    return new BooleanPrimitiveFleakData(search(haystack, needle));
  }

  private static boolean search(FleakData node, String needle) {
    switch (node) {
      case RecordFleakData record -> {
        for (FleakData child : record.getPayload().values()) {
          if (child != null && search(child, needle)) {
            return true;
          }
        }
        return false;
      }
      case ArrayFleakData array -> {
        for (FleakData child : array.getArrayPayload()) {
          if (child != null && search(child, needle)) {
            return true;
          }
        }
        return false;
      }
      default -> {
        Object raw = node.unwrap();
        return raw != null && String.valueOf(raw).toLowerCase().contains(needle);
      }
    }
  }
}
