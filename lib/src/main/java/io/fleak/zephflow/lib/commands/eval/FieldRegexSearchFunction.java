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

import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import java.util.List;
import java.util.regex.Pattern;

/** Searches all scalar occurrences of one logical inspection field. */
final class FieldRegexSearchFunction implements FeelFunction {
  private volatile CompiledPattern cached;

  private record CompiledPattern(String expression, Pattern pattern) {}

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required(
        "field_regex_search", 3, "record, logical field name and regex pattern");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext context,
      List<FleakData> arguments,
      EvalExpressionParser.GenericFunctionCallContext originalContext) {
    if (!(arguments.getFirst() instanceof RecordFleakData record)
        || !(arguments.get(1) instanceof StringPrimitiveFleakData field)
        || !(arguments.get(2) instanceof StringPrimitiveFleakData expression)) {
      throw new IllegalArgumentException(
          "field_regex_search expects a record, string field name and string regex");
    }
    String text = expression.getStringValue();
    CompiledPattern compiled = cached;
    if (compiled == null || !compiled.expression().equals(text)) {
      compiled = new CompiledPattern(text, FieldPatternMatcher.compile(text));
      cached = compiled;
    }
    return new BooleanPrimitiveFleakData(
        FieldPatternMatcher.evaluate(record, field.getStringValue(), compiled.pattern()).matched());
  }
}
