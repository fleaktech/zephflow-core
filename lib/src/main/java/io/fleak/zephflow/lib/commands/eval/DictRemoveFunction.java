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
dictRemoveFunction:
Remove specified keys from a dictionary and return a new dictionary.

Syntax:
```
dict_remove(dictionary, key1, key2, ...)
```

Parameters:
- dictionary: The input dictionary/record to remove keys from
- key1, key2, ...: One or more string keys to remove from the dictionary

Return Value:
- A new dictionary with the specified keys removed
- Non-existent keys are silently ignored

Examples:
```
dict_remove({"a": 1, "b": 2, "c": 3}, "b") returns {"a": 1, "c": 3}
dict_remove({"a": 1, "b": 2, "c": 3}, "b", "c") returns {"a": 1}
dict_remove({"a": 1}, "x") returns {"a": 1} (non-existent key ignored)
```
*/
class DictRemoveFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.variable("dict_remove", 2, "dictionary and keys to remove");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    if (evaluatedArgs.size() < 2) {
      throw new IllegalArgumentException(
          "dict_remove expects at least 2 arguments: dictionary and at least one key to remove");
    }

    FleakData dictData = evaluatedArgs.getFirst();
    if (dictData == null) {
      return null;
    }

    Preconditions.checkArgument(
        dictData instanceof RecordFleakData,
        "dict_remove: first argument must be a dictionary but found: %s",
        dictData.unwrap());

    Map<String, FleakData> resultPayload = new HashMap<>(dictData.getPayload());

    for (int i = 1; i < evaluatedArgs.size(); i++) {
      FleakData keyData = evaluatedArgs.get(i);
      if (keyData == null) {
        continue;
      }

      Preconditions.checkArgument(
          keyData instanceof StringPrimitiveFleakData,
          "dict_remove: key arguments must be strings but found: %s at position %d",
          keyData.unwrap(),
          i + 1);

      String keyToRemove = keyData.getStringValue();
      resultPayload.remove(keyToRemove);
    }

    return new RecordFleakData(resultPayload);
  }
}
