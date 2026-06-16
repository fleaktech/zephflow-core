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
dictMergeFunction:
Merge a set of dictionaries into one.
For example:
```
dict_merge(
  $,
  grok($.__raw__, "%{GREEDYDATA:timestamp} %{HOSTNAME:hostname} %{WORD:program}\\[%{POSINT:pid}\\]: %ASA-%{INT:level}-%{INT:message_number}: %{GREEDYDATA:message_text}")
)
```
(`$` refers to the current input event)

It's required that all arguments are dictionaries.
For every argument, add all its key value pairs to the result dictionary. Duplicated keys are overriden.

returns a merged dict
*/
class DictMergeFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.variable("dict_merge", 1, "one or more dictionaries");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    if (evaluatedArgs.isEmpty()) {
      throw new IllegalArgumentException("dict_merge expects at least 1 argument");
    }

    Map<String, FleakData> mergedPayload = new HashMap<>();
    for (FleakData fd : evaluatedArgs) {
      if (fd == null) {
        continue;
      }
      Preconditions.checkArgument(
          fd instanceof RecordFleakData,
          "dict_merge: every argument must be a record but found: %s",
          fd.unwrap());
      mergedPayload.putAll(fd.getPayload());
    }

    return new RecordFleakData(mergedPayload);
  }
}
