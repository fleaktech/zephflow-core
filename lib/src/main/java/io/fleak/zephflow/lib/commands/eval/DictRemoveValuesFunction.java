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
dictRemoveValuesFunction:
Recursively remove all entries whose value equals any of the given values.
Walks nested dictionaries and arrays; array elements matching a value are removed
from the array. Containers that become empty are kept. Returns a new structure;
the input is not mutated.

Syntax:
```
dict_remove_values(dictionary, value1, value2, ...)
```

Parameters:
- dictionary: The input dictionary/record to clean
- value1, value2, ...: One or more values; any entry/element equal to one of them is removed

Behavior:
- null dictionary returns null
- a null value argument removes entries/elements whose value is null
- equality is type-sensitive ("0" does not match 0); numbers compare by value (0 matches 0.0)

Examples:
```
dict_remove_values({"a": "0x0", "b": 1}, "0x0")    returns {"b": 1}
dict_remove_values({"a": {"x": "-", "y": 2}}, "-") returns {"a": {"y": 2}}
dict_remove_values({"a": [1, "-", 2]}, "-")        returns {"a": [1, 2]}
```
*/
class DictRemoveValuesFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.variable("dict_remove_values", 2, "dictionary and values to remove");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    if (evaluatedArgs.size() < 2) {
      throw new IllegalArgumentException(
          "dict_remove_values expects at least 2 arguments: dictionary and at least one value to remove");
    }

    FleakData dictData = evaluatedArgs.getFirst();
    if (dictData == null) {
      return null;
    }

    Preconditions.checkArgument(
        dictData instanceof RecordFleakData,
        "dict_remove_values: first argument must be a dictionary but found: %s",
        dictData.unwrap());

    List<FleakData> valuesToRemove = evaluatedArgs.subList(1, evaluatedArgs.size());
    return cleanRecord((RecordFleakData) dictData, valuesToRemove);
  }

  private static RecordFleakData cleanRecord(RecordFleakData record, List<FleakData> nonValues) {
    Map<String, FleakData> resultPayload = new HashMap<>();
    for (Map.Entry<String, FleakData> entry : record.getPayload().entrySet()) {
      if (matches(entry.getValue(), nonValues)) {
        continue;
      }
      resultPayload.put(entry.getKey(), cleanValue(entry.getValue(), nonValues));
    }
    return new RecordFleakData(resultPayload);
  }

  private static FleakData cleanValue(FleakData value, List<FleakData> nonValues) {
    return switch (value) {
      case RecordFleakData record -> cleanRecord(record, nonValues);
      case ArrayFleakData array ->
          new ArrayFleakData(
              array.getArrayPayload().stream()
                  .filter(elem -> !matches(elem, nonValues))
                  .map(elem -> cleanValue(elem, nonValues))
                  .toList());
      case null, default -> value;
    };
  }

  private static boolean matches(FleakData value, List<FleakData> nonValues) {
    return nonValues.stream().anyMatch(nonValue -> Objects.equals(value, nonValue));
  }
}
