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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.cli.ParseException;

/** Created by bolei on 1/10/25 */
public class Main {
  public static void main(String[] args) throws ParseException {

    EvaluatorConfig config;
    config = EvaluatorConfig.parse(args);
    ExpressionEvaluator evaluator = ExpressionEvaluator.create(config.getGrammarType());
    Map<String, Object> result = new HashMap<>();
    for (String expr : config.getExpressions()) {
      try {
        FleakData output = evaluator.evaluateExpression(expr, config.getInputEvent());
        result.put(expr, Optional.ofNullable(output).map(FleakData::unwrap).orElse(null));
      } catch (Exception e) {
        result.put(expr, ">>> Failed to evaluate: " + expr);
      }
    }
    System.out.println(toJsonString(result));
  }
}
