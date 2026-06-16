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
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.TimeZone;
import org.graalvm.polyglot.*;

/*
epochToTsStrFunction:
Convert an epoch millisecond timestamp into a human readable string.
The output string is formatted in UTC timezone.

Syntax:
```
epoch_to_ts_str($.path.to.timestamp.field, "<date_time_pattern>")
```
where
- $.path.to.timestamp.field points to the field that contains the epoch millis timestamp
- "<date_time_pattern>" is a string literal that represents Unicode Date Format Patterns
The implementation uses java `SimpleDateFormat` to stringify the epoch millis timestamp
*/
class EpochToTsStrFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("epoch_to_ts_str", 2, "timestamp epoch and date pattern");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData epochFd = evaluatedArgs.getFirst();
    Preconditions.checkArgument(
        epochFd instanceof NumberPrimitiveFleakData,
        "epoch_to_ts_str: timestamp field to be parsed is not a number: %s",
        epochFd);

    long epoch = (long) epochFd.getNumberValue();
    FleakData patternFd = evaluatedArgs.get(1);
    Preconditions.checkArgument(
        patternFd instanceof StringPrimitiveFleakData,
        "epoch_to_ts_str: pattern must be a string: %s",
        patternFd);
    String patternStr = patternFd.getStringValue();

    SimpleDateFormat simpleDateFormat;
    try {
      simpleDateFormat =
          DateFormatCache.getCachedDateFormat(patternStr, TimeZone.getTimeZone("UTC"));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "epoch_to_ts_str: failed to process date time pattern: " + patternStr);
    }

    String tsStr = simpleDateFormat.format(new Date(epoch));
    return new StringPrimitiveFleakData(tsStr);
  }
}
