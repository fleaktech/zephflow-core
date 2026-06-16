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
sizeFunction:
return the size of the argument. Supported input argument types
- array: return number of elements in the array
- object/dict/map: return number of key-value pairs
- string: return the string size
*/
class SizeOfFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("size_of", 1, "array, object, or string");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    return evaluateSizeOf(evaluatedArgs.getFirst());
  }

  private FleakData evaluateSizeOf(FleakData arg) {
    if (arg == null) {
      return null;
    }
    if (arg instanceof RecordFleakData) {
      return new NumberPrimitiveFleakData(
          arg.getPayload().size(), NumberPrimitiveFleakData.NumberType.LONG);
    }
    if (arg instanceof ArrayFleakData) {
      return new NumberPrimitiveFleakData(
          arg.getArrayPayload().size(), NumberPrimitiveFleakData.NumberType.LONG);
    }
    if (arg instanceof StringPrimitiveFleakData) {
      return new NumberPrimitiveFleakData(
          arg.getStringValue().length(), NumberPrimitiveFleakData.NumberType.LONG);
    }
    throw new IllegalArgumentException("Unsupported argument: " + arg);
  }
}
