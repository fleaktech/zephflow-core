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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.TimeZone;
import org.graalvm.polyglot.*;

/*
tsStrToEpochFunction:
Convert a datetime string to epoch milliseconds.
The timestamp string is interpreted as UTC timezone.

Syntax:
```
ts_str_to_epoch($.path.to.timestamp.field, "<date_time_pattern>")
```
where
- `$.path.to.timestamp.field` points to the field that contains the timestamp string value
- `<date_time_pattern>` is a string literal that represents Unicode Date Format Patterns
The implementation uses java `SimpleDateFormat` to parse the timestamp string
*/
class TsStrToEpochFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("ts_str_to_epoch", 2, "timestamp string and date pattern");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    if (evaluatedArgs.size() != 2) {
      throw new IllegalArgumentException("ts_str_to_epoch expects 2 arguments");
    }

    FleakData timestampStrFd = evaluatedArgs.getFirst();
    Preconditions.checkArgument(
        timestampStrFd instanceof StringPrimitiveFleakData,
        "ts_str_to_epoch: timestamp field to be parsed is not a string: %s",
        timestampStrFd);

    String tsStr = timestampStrFd.getStringValue();
    FleakData patternFd = evaluatedArgs.get(1);
    Preconditions.checkArgument(
        patternFd instanceof StringPrimitiveFleakData,
        "ts_str_to_epoch: pattern must be a string: %s",
        patternFd);
    String patternStr = patternFd.getStringValue();

    SimpleDateFormat simpleDateFormat;
    try {
      simpleDateFormat =
          DateFormatCache.getCachedDateFormat(patternStr, TimeZone.getTimeZone("UTC"));
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "ts_str_to_epoch: failed to process date time pattern: " + patternStr);
    }

    try {
      Date date = simpleDateFormat.parse(tsStr);
      return new NumberPrimitiveFleakData(date.getTime(), NumberPrimitiveFleakData.NumberType.LONG);
    } catch (ParseException e) {
      throw new IllegalArgumentException(
          String.format(
              "ts_str_to_epoch: failed to parse timestamp string %s with pattern %s",
              tsStr, patternStr));
    }
  }
}
