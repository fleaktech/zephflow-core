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
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.IsoFields;
import java.util.List;

/*
dateComponentEqAnyFunction:
True if the calendar component (minute/hour/day/week/month/year, UTC) of the value equals the given
number. The value may be an epoch-millisecond number or an ISO-8601 string, and may be a scalar OR
an array, in which case any element satisfying it returns true.

Syntax:
date_component_eq_any($.path.to.timestamp, "hour", 14)
*/
class DateComponentEqAnyFunction implements FeelFunction {

  @Override
  public FunctionSignature getSignature() {
    return FunctionSignature.required("date_component_eq_any", 3, "timestamp, unit, number");
  }

  @Override
  public FleakData evaluateCompiledEager(
      EvalContext ctx,
      List<FleakData> evaluatedArgs,
      EvalExpressionParser.GenericFunctionCallContext originalCtx) {
    FleakData unitFd = evaluatedArgs.get(1);
    Preconditions.checkArgument(
        unitFd instanceof StringPrimitiveFleakData,
        "date_component_eq_any: unit must be a string: %s",
        unitFd);
    FleakData numberFd = evaluatedArgs.get(2);
    Preconditions.checkArgument(
        numberFd instanceof NumberPrimitiveFleakData,
        "date_component_eq_any: number must be a number: %s",
        numberFd);

    return new BooleanPrimitiveFleakData(
        matchesAny(
            evaluatedArgs.getFirst(), unitFd.getStringValue(), (int) numberFd.getNumberValue()));
  }

  private static boolean matchesAny(FleakData value, String unit, int expected) {
    if (value == null) {
      return false;
    }
    if (value instanceof ArrayFleakData array) {
      for (FleakData element : array.getArrayPayload()) {
        if (matchesAny(element, unit, expected)) {
          return true;
        }
      }
      return false;
    }
    ZonedDateTime dateTime = toUtcDateTime(value);
    if (dateTime == null) {
      return false;
    }
    Integer component = extract(dateTime, unit);
    return component != null && component == expected;
  }

  private static ZonedDateTime toUtcDateTime(FleakData valueFd) {
    if (valueFd instanceof NumberPrimitiveFleakData) {
      return Instant.ofEpochMilli((long) valueFd.getNumberValue()).atZone(ZoneOffset.UTC);
    }
    if (valueFd instanceof StringPrimitiveFleakData) {
      String str = valueFd.getStringValue().trim();
      try {
        return OffsetDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME)
            .atZoneSameInstant(ZoneOffset.UTC);
      } catch (RuntimeException ignored) {
        // fall through
      }
      try {
        return Instant.parse(str).atZone(ZoneOffset.UTC);
      } catch (RuntimeException ignored) {
        return null;
      }
    }
    return null;
  }

  private static Integer extract(ZonedDateTime dateTime, String unit) {
    return switch (unit.toLowerCase()) {
      case "minute" -> dateTime.getMinute();
      case "hour" -> dateTime.getHour();
      case "day" -> dateTime.getDayOfMonth();
      case "week" -> dateTime.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
      case "month" -> dateTime.getMonthValue();
      case "year" -> dateTime.getYear();
      default -> null;
    };
  }
}
