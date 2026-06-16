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

import com.google.common.collect.ImmutableMap;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionNode;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import java.util.List;
import java.util.Map;

/** Created by bolei on 9/2/25 */
public interface FeelFunction {

  FunctionSignature getSignature();

  default boolean isLazyEvaluation() {
    return false;
  }

  default FleakData evaluateCompiled(
      EvalContext ctx,
      List<ExpressionNode> args,
      EvalExpressionParser.GenericFunctionCallContext originalCtx,
      List<String> lazyArgTexts) {
    throw new UnsupportedOperationException(
        "Lazy compiled evaluation not implemented for " + getSignature().functionName());
  }

  default FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    throw new UnsupportedOperationException(
        "Eager compiled evaluation not implemented for " + getSignature().functionName());
  }

  // Helper method to create FUNCTIONS_TABLE with optional PythonExecutor
  static Map<String, FeelFunction> createFunctionsTable(PythonExecutor pythonExecutor) {
    var builder =
        ImmutableMap.<String, FeelFunction>builder()
            .put("ts_str_to_epoch", new TsStrToEpochFunction())
            .put("epoch_to_ts_str", new EpochToTsStrFunction())
            .put("str_contains", new StrContainsFunction())
            .put("regex_match", new RegexMatchFunction())
            .put("to_str", new ToStringFunction())
            .put("upper", new UpperFunction())
            .put("lower", new LowerFunction())
            .put("size_of", new SizeOfFunction())
            .put("grok", new GrokFunction())
            .put("parse_int", new ParseIntFunction())
            .put("parse_float", new ParseFloatFunction())
            .put("array", new ArrayFunction())
            .put("str_split", new StrSplitFunction())
            .put("str_replace", new StrReplaceFunction())
            .put("substr", new SubstrFunction())
            .put("duration_str_to_mills", new DurationStrToMillsFunction())
            .put("arr_flatten", new ArrFlattenFunction())
            .put("range", new RangeFunction())
            .put("arr_foreach", new ArrForEachFunction())
            .put("arr_find", new ArrFindFunction())
            .put("arr_filter", new ArrFilterFunction())
            .put("arr_to_dict", new ArrToDictFunction())
            .put("dict_merge", new DictMergeFunction())
            .put("dict_remove", new DictRemoveFunction())
            .put("dict_remove_values", new DictRemoveValuesFunction())
            .put("dict_set", new DictSetFunction())
            .put("floor", new FloorFunction())
            .put("ceil", new CeilFunction())
            .put("now", new NowFunction())
            .put("random_long", new RandomLongFunction())
            .put("in", new InFunction());

    if (pythonExecutor != null) {
      builder.put("python", new PythonFunction(pythonExecutor));
    }

    return builder.build();
  }
}
