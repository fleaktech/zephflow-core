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
package io.fleak.zephflow.evaluator;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.AntlrUtils;

/** Created by bolei on 1/10/25 */
public abstract class ExpressionEvaluator {
  public static ExpressionEvaluator create(AntlrUtils.GrammarType grammarType) {
    return switch (grammarType) {
      case EVAL -> new EvalExpressionEvaluator();
      case EXPRESSION_SQL -> new SqlExpressionEvaluator();
    };
  }

  public FleakData evaluateExpression(String expression, RecordFleakData event) {
    try {

      return doEvaluation(expression, event);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              """
Failed to evaluate
===expression:
%s
===input event:
%s
""",
              expression, toJsonString(event.unwrap())),
          e);
    }
  }

  protected abstract FleakData doEvaluation(String expression, RecordFleakData event);
}
