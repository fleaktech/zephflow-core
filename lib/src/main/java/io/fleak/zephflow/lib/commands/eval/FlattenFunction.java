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

/*
flattenFunction:
Flatten a nested dictionary into a single-level dictionary. Nested dictionary keys are
joined with a delimiter, and array elements get their zero-based index as a key segment.
Use arr_flatten instead to remove one level of nesting from an array.

Syntax:
```
flatten(dictionary)
flatten(dictionary, delimiter)
flatten(dictionary, delimiter, depth)
```

Parameters:
- dictionary: The input dictionary/record to flatten
- delimiter: Optional non-empty string placed between key segments. Defaults to "_"
- depth: Optional integer >= 1, the maximum number of levels descended below each top-level key,
  so a key has at most depth + 1 segments. Defaults to 5

Behavior:
- null dictionary returns null
- a non-dictionary first argument, an empty or non-string delimiter, or a depth that is not
  an integer >= 1 raises an error
- flattened keys that would total more than 10,000,000 characters raise an error
- descent stops after depth levels; the value at the cutoff is kept unchanged
- scalars, nulls, empty dictionaries, and empty arrays are kept as values, never dropped
- when two flattened keys collide, the one with fewer levels wins, so a literal "a_b" key
  beats a nested a.b; a tie goes to the path that sorts first. No error is raised
- returns a new dictionary; the input is not mutated

Examples:
```
flatten({"a": {"b": {"c": 1}}})              returns {"a_b_c": 1}
flatten({"a": {"b": {"c": 1}}}, ".")         returns {"a.b.c": 1}
flatten({"a": {"b": {"c": 1}}}, "_", 1)      returns {"a_b": {"c": 1}}
flatten({"acc": [{"n": "John"}, {"n": "Mary"}], "host": "h1"})
                                             returns {"acc_0_n": "John", "acc_1_n": "Mary", "host": "h1"}
flatten({"a": {}, "b": [], "c": null})       returns {"a": {}, "b": [], "c": null}
flatten(dict(res=$.resource))                prefixes every key with "res_"
```
*/
class FlattenFunction implements FeelFunction {
  private static final String DEFAULT_DELIMITER = "_";
  private static final int DEFAULT_DEPTH = 5;

  // Joined keys repeat their prefixes, so key text can grow quadratically with the input: one
  // 50,000-character key above 100,000 leaves would be 5 billion characters.
  private static final long MAX_TOTAL_KEY_LENGTH = 10_000_000;

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.optional("flatten", 1, 3, "dictionary, delimiter, and depth");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData dictData = evaluatedArgs.getFirst();
    if (dictData == null) {
      return null;
    }

    // Not Preconditions: its message argument would deep-unwrap the whole input on every call.
    if (!(dictData instanceof RecordFleakData record)) {
      throw new IllegalArgumentException(
          "flatten: first argument must be a dictionary but found: " + dictData.unwrap());
    }

    String delimiter = DEFAULT_DELIMITER;
    if (evaluatedArgs.size() > 1) {
      FleakData delimiterData = evaluatedArgs.get(1);
      Preconditions.checkArgument(
          delimiterData instanceof StringPrimitiveFleakData
              && !delimiterData.getStringValue().isEmpty(),
          "flatten: delimiter must be a non-empty string but found: %s",
          delimiterData == null ? null : delimiterData.unwrap());
      delimiter = delimiterData.getStringValue();
    }

    int depth = DEFAULT_DEPTH;
    if (evaluatedArgs.size() > 2) {
      FleakData depthData = evaluatedArgs.get(2);
      Preconditions.checkArgument(
          depthData instanceof NumberPrimitiveFleakData
              && depthData.getNumberValue() == Math.rint(depthData.getNumberValue())
              && depthData.getNumberValue() >= 1,
          "flatten: depth must be an integer >= 1 but found: %s",
          depthData == null ? null : depthData.unwrap());
      depth = (int) Math.min(depthData.getNumberValue(), Integer.MAX_VALUE);
    }

    return new RecordFleakData(new Flattener(delimiter).run(record, depth));
  }

  private record Pending(String key, FleakData value, int remainingDepth) {}

  /**
   * Walks breadth-first over sorted keys, so a key with fewer levels is written first and wins a
   * collision, and a tie goes to the path that sorts first. Iterative, so deep input cannot
   * overflow the stack.
   */
  private static final class Flattener {
    private final String delimiter;
    private final Deque<Pending> queue = new ArrayDeque<>();
    private final Map<String, FleakData> result = new HashMap<>();
    private long totalKeyLength;

    Flattener(String delimiter) {
      this.delimiter = delimiter;
    }

    Map<String, FleakData> run(RecordFleakData record, int depth) {
      for (Map.Entry<String, FleakData> entry : sortedEntries(record)) {
        queue.add(new Pending(entry.getKey(), entry.getValue(), depth));
      }
      while (!queue.isEmpty()) {
        Pending next = queue.poll();
        boolean expand = next.remainingDepth() > 0;
        if (expand
            && next.value() instanceof RecordFleakData child
            && !child.getPayload().isEmpty()) {
          for (Map.Entry<String, FleakData> entry : sortedEntries(child)) {
            enqueue(next, entry.getKey(), entry.getValue());
          }
        } else if (expand
            && next.value() instanceof ArrayFleakData array
            && !array.getArrayPayload().isEmpty()) {
          List<FleakData> elements = array.getArrayPayload();
          for (int i = 0; i < elements.size(); i++) {
            enqueue(next, String.valueOf(i), elements.get(i));
          }
        } else if (!result.containsKey(next.key())) {
          result.put(next.key(), next.value());
        }
      }
      return result;
    }

    private void enqueue(Pending parent, String segment, FleakData value) {
      totalKeyLength += (long) parent.key().length() + delimiter.length() + segment.length();
      if (totalKeyLength > MAX_TOTAL_KEY_LENGTH) {
        throw new IllegalArgumentException(
            "flatten: the flattened keys would exceed "
                + MAX_TOTAL_KEY_LENGTH
                + " characters in total; flatten a smaller part of the event or lower the depth");
      }
      queue.add(
          new Pending(parent.key() + delimiter + segment, value, parent.remainingDepth() - 1));
    }

    private static List<Map.Entry<String, FleakData>> sortedEntries(RecordFleakData record) {
      return record.getPayload().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();
    }
  }
}
