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
dictSetFunction:
Set one or more key-value pairs on a dictionary and return a new dictionary.
Keys are evaluated at runtime, so dynamically computed key names are supported
(unlike the dict() literal whose keys are static). Existing keys are overwritten.

Syntax:
```
dict_set(dictionary, key1, value1, key2, value2, ...)
```

Parameters:
- dictionary: The input dictionary/record to set fields on
- key1, value1, ...: One or more key-value pairs; keys must be strings

Behavior:
- null dictionary returns null
- a null key skips that pair
- a null value sets the field to null
- setting an existing key overwrites its value; duplicate keys in one call: last wins
- returns a new dictionary; the input is not mutated

Examples:
```
dict_set({"a": 1}, "b", 2)            returns {"a": 1, "b": 2}
dict_set({"a": 1}, "a", 2, "b", 3)    returns {"a": 2, "b": 3}
dict_set($, $.field_name, $.value)    sets a dynamically named field
```
*/
class DictSetFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.variable("dict_set", 3, "dictionary and key-value pairs to set");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    if (evaluatedArgs.size() < 3 || evaluatedArgs.size() % 2 == 0) {
      throw new IllegalArgumentException(
          "dict_set expects a dictionary followed by one or more key-value pairs");
    }

    FleakData dictData = evaluatedArgs.getFirst();
    if (dictData == null) {
      return null;
    }

    Preconditions.checkArgument(
        dictData instanceof RecordFleakData,
        "dict_set: first argument must be a dictionary but found: %s",
        dictData.unwrap());

    Map<String, FleakData> resultPayload = new HashMap<>(dictData.getPayload());

    for (int i = 1; i < evaluatedArgs.size(); i += 2) {
      FleakData keyData = evaluatedArgs.get(i);
      if (keyData == null) {
        continue;
      }

      Preconditions.checkArgument(
          keyData instanceof StringPrimitiveFleakData,
          "dict_set: key arguments must be strings but found: %s at position %d",
          keyData.unwrap(),
          i + 1);

      resultPayload.put(keyData.getStringValue(), evaluatedArgs.get(i + 1));
    }

    return new RecordFleakData(resultPayload);
  }
}
