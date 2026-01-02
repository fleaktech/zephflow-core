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
package io.fleak.zephflow.lib.commands.eval.compiled.nodes;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Represents a dict expression: dict(k1=v1, k2=v2, ...) */
public record DictNode(List<KvPair> kvPairs) implements ExpressionNode {

  public record KvPair(String key, ExpressionNode valueNode, String originalExprText) {}

  @Override
  public FleakData evaluate(EvalContext ctx) {
    Map<String, FleakData> payload = new HashMap<>();
    for (KvPair kvPair : kvPairs) {
      FleakData value;
      try {
        value = kvPair.valueNode().evaluate(ctx);
      } catch (Exception e) {
        if (ctx.lenient()) {
          value =
              FleakData.wrap(
                  String.format(
                      ">>> Failed to evaluate expression: %s. Reason: %s",
                      kvPair.originalExprText(), e.getMessage()));
        } else {
          throw e;
        }
      }
      payload.put(kvPair.key(), value);
    }
    return new RecordFleakData(payload);
  }
}
