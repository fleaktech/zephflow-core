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

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.EvalContext;
import io.fleak.zephflow.lib.serdes.decode.auto.FormatRecord;
import java.util.List;

final class FormatRecordFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required(
        "format_record", 1, "Project a controlled format envelope to one record");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext context,
      List<FleakData> arguments,
      EvalExpressionParser.GenericFunctionCallContext originalContext) {
    if (arguments.size() != 1 || !(arguments.getFirst() instanceof RecordFleakData record)) {
      throw new IllegalArgumentException("format_record expects one format envelope");
    }
    return FormatRecord.project(record);
  }
}
