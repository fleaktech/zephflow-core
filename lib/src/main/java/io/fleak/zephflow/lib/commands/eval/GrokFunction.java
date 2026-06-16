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
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.*;
import org.opensearch.grok.Grok;

/*
grokFunction:
Apply grok pattern to a given string field and return a dictionary with all grok extracted fields

Syntax:
grok($.path.to.string.field, "<grok_pattern>")

For example:
Given the following input event:
```
{
  "__raw__": "Oct 10 2018 12:34:56 localhost CiscoASA[999]: %ASA-6-305011: Built dynamic TCP translation from inside:172.31.98.44/1772 to outside:100.66.98.44/8256"
}
```

And the grok function:
```
grok($.__raw__, "%{GREEDYDATA:timestamp} %{HOSTNAME:hostname} %{WORD:program}\\[%{POSINT:pid}\\]: %ASA-%{INT:level}-%{INT:message_number}: %{GREEDYDATA:message_text}")
```

Results:
```
{
    "timestamp": "Oct 10 2018 12:34:56",
    "hostname": "localhost",
    "program": "CiscoASA",
    "pid": "999",
    "level": "6",
    "message_number": "305011",
    "message_text": "Built dynamic TCP translation from inside:172.31.98.44/1772 to outside:100.66.98.44/8256"
}
```
*/
@Slf4j
class GrokFunction implements FeelFunction {
  private final Map<String, Grok> grokCache = new HashMap<>();

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("grok", 2, "input string and grok pattern");
  }

  private Grok getOrCreateGrok(String pattern) {
    return grokCache.computeIfAbsent(pattern, k -> new Grok(Grok.BUILTIN_PATTERNS, k, log::debug));
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData targetValue = evaluatedArgs.getFirst();
    if (targetValue == null) {
      return new RecordFleakData();
    }
    String targetValueStr = targetValue.getStringValue();

    FleakData patternFd = evaluatedArgs.get(1);
    Preconditions.checkArgument(
        patternFd instanceof StringPrimitiveFleakData,
        "grok: pattern must be a string: %s",
        patternFd);
    String grokPattern = patternFd.getStringValue();
    Grok grok = getOrCreateGrok(grokPattern);

    Map<String, Object> map = grok.captures(targetValueStr);
    return FleakData.wrap(map);
  }
}
