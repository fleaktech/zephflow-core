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
durationStrToMillsFunction:
Convert a duration string in the format of `HH:mm:ss` to milliseconds

For example:
```
duration_str_to_mills("0:01:07")
```
returns `67000`

*/
class DurationStrToMillsFunction implements FeelFunction {
  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("duration_str_to_mills", 1, "duration string");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData durStrFd = evaluatedArgs.getFirst();
    Preconditions.checkArgument(
        durStrFd instanceof StringPrimitiveFleakData,
        "duration_str_to_mills: duration argument is not a string: %s",
        durStrFd);

    String durationStr = durStrFd.getStringValue();
    if (durationStr == null || durationStr.isEmpty()) {
      throw new IllegalArgumentException("Duration string cannot be null or empty");
    }

    String[] parts = durationStr.split(":");
    if (parts.length != 3) {
      throw new IllegalArgumentException(
          String.format("Invalid duration format: %s . Expected format is hh:mm:ss", durationStr));
    }

    try {
      long duration = getDuration(parts);
      return new NumberPrimitiveFleakData(duration, NumberPrimitiveFleakData.NumberType.LONG);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Duration string contains non-numeric characters", e);
    }
  }

  private long getDuration(String[] parts) {
    int hours = Integer.parseInt(parts[0].trim());
    int minutes = Integer.parseInt(parts[1].trim());
    int seconds = Integer.parseInt(parts[2].trim());

    if (hours < 0) {
      throw new IllegalArgumentException("Hours value cannot be negative");
    }
    if (minutes < 0 || minutes >= 60) {
      throw new IllegalArgumentException("Minutes value must be between 0 and 59");
    }
    if (seconds < 0 || seconds >= 60) {
      throw new IllegalArgumentException("Seconds value must be between 0 and 59");
    }

    return ((hours * 3600L) + (minutes * 60L) + seconds) * 1000L;
  }
}
