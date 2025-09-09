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

import static io.fleak.zephflow.lib.utils.GraalUtils.graalValueToFleakData;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.python.CompiledPythonFunction;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.graalvm.polyglot.*;
import org.opensearch.grok.Grok;

/** Created by bolei on 9/2/25 */
public interface FeelFunction {

  record FunctionSignature(String functionName, int minArgs, int maxArgs, String description) {
    public static FunctionSignature required(String name, int argCount, String desc) {
      return new FunctionSignature(name, argCount, argCount, desc);
    }

    public static FunctionSignature optional(String name, int minArgs, int maxArgs, String desc) {
      return new FunctionSignature(name, minArgs, maxArgs, desc);
    }

    public static FunctionSignature variable(String name, int minArgs, String desc) {
      return new FunctionSignature(name, minArgs, -1, desc);
    }
  }

  FunctionSignature getSignature();

  FleakData evaluate(
      ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args);

  private static FleakData visitExpression(
      ExpressionValueVisitor visitor, EvalExpressionParser.ExpressionContext ctx) {
    return visitor.visit(ctx);
  }

  private static String normalizeStrLiteral(String text) {
    String cleaned = cleanWrappedString(text);
    // Handle escaped characters properly
    return cleaned
        .replace("\\\\", "\\") // Convert \\\\ to \\
        .replace("\\'", "'"); // Convert \' to '
  }

  // Note: Removed duplicate evalArgAsInt - functions now use visitor.evalArgAsInt() for canonical
  // validation

  private static String cleanWrappedString(String v) {
    if (v == null || v.length() < 2) {
      return v;
    }

    char firstChar = v.charAt(0);
    char lastChar = v.charAt(v.length() - 1);

    // Handle both single and double quotes as defined in QUOTED_IDENTIFIER grammar
    if ((firstChar == '"' && lastChar == '"') || (firstChar == '\'' && lastChar == '\'')) {
      return v.substring(1, v.length() - 1);
    }

    return v;
  }

  /*
  tsStrToEpochFunction:
  Convert a datetime string input epoch milliseconds.
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
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 2) {
        throw new IllegalArgumentException(
            String.format(
                "ts_str_to_epoch expects 2 arguments but received %d. "
                    + "Usage: ts_str_to_epoch(timestamp_string, date_pattern). "
                    + "Example: ts_str_to_epoch(\"2023-12-25\", \"yyyy-MM-dd\")",
                args.size()));
      }

      FleakData timestampStrFd = visitExpression(visitor, args.get(0));
      Preconditions.checkArgument(
          timestampStrFd instanceof StringPrimitiveFleakData,
          "ts_str_to_epoch: timestamp field to be parsed is not a string: %s",
          timestampStrFd);

      String tsStr = timestampStrFd.getStringValue();
      // For now, assume the second argument is a quoted identifier (will need special handling)
      String patternStr = normalizeStrLiteral(args.get(1).getText());

      SimpleDateFormat simpleDateFormat;
      try {
        simpleDateFormat = new SimpleDateFormat(patternStr, Locale.US);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
      } catch (Exception e) {
        throw new RuntimeException(
            "ts_str_to_epoch: failed to process date time pattern: " + patternStr);
      }

      try {
        Date date = simpleDateFormat.parse(tsStr);
        return new NumberPrimitiveFleakData(
            date.getTime(), NumberPrimitiveFleakData.NumberType.LONG);
      } catch (ParseException e) {
        throw new RuntimeException(
            String.format(
                "ts_str_to_epoch: failed to parse timestamp string %s with pattern %s",
                tsStr, patternStr));
      }
    }
  }

  /*
  strContainsFunction:
  test if the given string contains a substring.

  Syntax:
  str_contains(str, sub_str)

  return `true` if `str` contains `sub_str`, otherwise `false`.
  both arguments are required to be of string type
  */
  class StrContainsFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("str_contains", 2, "string and substring");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 2) {
        throw new IllegalArgumentException(
            String.format(
                "str_contains expects 2 arguments but received %d. "
                    + "Usage: str_contains(string, substring). "
                    + "Example: str_contains(\"hello world\", \"world\")",
                args.size()));
      }

      FleakData val1 = visitExpression(visitor, args.get(0));
      FleakData val2 = visitExpression(visitor, args.get(1));
      boolean contains = val1.getStringValue().contains(val2.getStringValue());
      return FleakData.wrap(contains);
    }
  }

  /*
  toStringFunction:
  convert the input argument to String.
  If argument is null, return null (not `"null"`)
  */
  class ToStringFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("to_str", 1, "value to convert");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 1) {
        throw new IllegalArgumentException("to_str expects exactly 1 argument");
      }

      FleakData arg = visitExpression(visitor, args.get(0));
      if (arg == null) {
        return null;
      }
      return FleakData.wrap(Objects.toString(arg.unwrap()));
    }
  }

  /*
  UpperFunction:
  convert string to upper case. argument must be a string
  */
  class UpperFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("upper", 1, "string to uppercase");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 1) {
        throw new IllegalArgumentException("upper expects 1 argument");
      }

      FleakData arg = visitExpression(visitor, args.get(0));
      return FleakData.wrap(arg.getStringValue().toUpperCase());
    }
  }

  /*
  LowerFunction:
  convert string to lower case. argument must be a string
  */
  class LowerFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("lower", 1, "string to lowercase");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 1) {
        throw new IllegalArgumentException("lower expects 1 argument");
      }

      FleakData arg = visitExpression(visitor, args.get(0));
      return FleakData.wrap(arg.getStringValue().toLowerCase());
    }
  }

  /*
  sizeFunction:
  return the size of the argument. Supported input argument types
  - array: return number of elements in the array
  - object/dict/map: return number of key-value pairs
  - string: return the string size
  */
  class SizeOfFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("size_of", 1, "array, object, or string");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 1) {
        throw new IllegalArgumentException("size_of expects 1 argument");
      }

      FleakData arg = visitExpression(visitor, args.get(0));
      if (arg instanceof RecordFleakData) {
        return new NumberPrimitiveFleakData(
            arg.getPayload().size(), NumberPrimitiveFleakData.NumberType.LONG);
      }
      if (arg instanceof ArrayFleakData) {
        return new NumberPrimitiveFleakData(
            arg.getArrayPayload().size(), NumberPrimitiveFleakData.NumberType.LONG);
      }
      if (arg instanceof StringPrimitiveFleakData) {
        return new NumberPrimitiveFleakData(
            arg.getStringValue().length(), NumberPrimitiveFleakData.NumberType.LONG);
      }
      throw new IllegalArgumentException("Unsupported argument: " + arg);
    }
  }

  /*
  grokFunction:
  Apply grok pattern to a given string field and return a dictionary with all grok extracted fields

  Syntax:
  grok($.path.to.string.field, "<grok_pattern>")

  For example:
  Given the following input event:
  ```
  {
    "__raw__": "Oct 10 2018 12:34:56 localhost CiscoASA[999]: %ASA-6-305011: Built dynamic TCP translation from inside:172.31.98.44/1772 to outside:100.66.98.44/8256"
  }
  ```

  And the grok function:
  ```
  grok($.__raw__, "%{GREEDYDATA:timestamp} %{HOSTNAME:hostname} %{WORD:program}\\[%{POSINT:pid}\\]: %ASA-%{INT:level}-%{INT:message_number}: %{GREEDYDATA:message_text}")
  ```

  Results:
  ```
  {
      "timestamp": "Oct 10 2018 12:34:56",
      "hostname": "localhost",
      "program": "CiscoASA",
      "pid": "999",
      "level": "6",
      "message_number": "305011",
      "message_text": "Built dynamic TCP translation from inside:172.31.98.44/1772 to outside:100.66.98.44/8256"
  }
  ```
  */
  @Slf4j
  class GrokFunction implements FeelFunction {
    private final Map<String, Grok> grokCache = new HashMap<>();

    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("grok", 2, "input string and grok pattern");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 2) {
        throw new IllegalArgumentException(
            String.format(
                "grok expects 2 arguments but received %d. "
                    + "Usage: grok(input_string, grok_pattern). "
                    + "Example: grok($.__raw__, \"%%{WORD:program} %%{GREEDYDATA:message}\")",
                args.size()));
      }

      FleakData targetValue = visitExpression(visitor, args.get(0));
      if (targetValue == null) {
        return new RecordFleakData();
      }
      String targetValueStr = targetValue.getStringValue();
      String grokPattern = normalizeStrLiteral(args.get(1).getText());

      Grok grok =
          grokCache.computeIfAbsent(
              grokPattern, k -> new Grok(Grok.BUILTIN_PATTERNS, k, log::debug));
      Map<String, Object> map = grok.captures(targetValueStr);
      return FleakData.wrap(map);
    }
  }

  /*
  parseIntFunction:
  Parse a string into an integer. It's equivalent to Java `Long.parseLong()`

  For example:
  ```
  parse_int("3")
  ```
  returns `3`.
  */
  class ParseIntFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.optional("parse_int", 1, 2, "string to parse and optional radix");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.isEmpty() || args.size() > 2) {
        throw new IllegalArgumentException("parse_int expects 1 or 2 arguments");
      }

      FleakData valueFd = visitExpression(visitor, args.get(0));
      Preconditions.checkArgument(
          valueFd instanceof StringPrimitiveFleakData,
          "parse_int: argument to be parsed is not a string: %s",
          valueFd);

      String intStr = valueFd.getStringValue();
      int radix = 10;

      if (args.size() == 2) {
        // Second argument should be an INT_LITERAL token
        String radixStr = args.get(1).getText();
        radix = Integer.parseInt(radixStr);
      }

      try {
        long value = Long.parseLong(intStr, radix);
        return new NumberPrimitiveFleakData(value, NumberPrimitiveFleakData.NumberType.LONG);
      } catch (Exception e) {
        throw new RuntimeException(
            "parse_int: failed to parse int string: " + intStr + " with radix: " + radix);
      }
    }
  }

  /*
  parseFloatFunction:
  Parse a string into an float number. It's equivalent to Java `Double.parseDouble()`

  For example:
  ```
  parse_float("3.14")
  ```
  returns `3.14`.
  */
  class ParseFloatFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("parse_float", 1, "string to parse");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 1) {
        throw new IllegalArgumentException("parse_float expects 1 argument");
      }

      FleakData valueFd = visitExpression(visitor, args.get(0));
      Preconditions.checkArgument(
          valueFd instanceof StringPrimitiveFleakData,
          "parse_float: argument to be parsed is not a string: %s",
          valueFd);

      String numberStr = valueFd.getStringValue();
      NumberFormat format = NumberFormat.getInstance(Locale.US);
      try {
        Number number = format.parse(numberStr);
        return new NumberPrimitiveFleakData(
            number.doubleValue(), NumberPrimitiveFleakData.NumberType.DOUBLE);
      } catch (Exception e) {
        throw new RuntimeException("parse_float: failed to parse float string: " + numberStr);
      }
    }
  }

  /*
  arrayFunction:
  Evaluate each argument and combine the result values into an array.
  For example:
  Given an input event:
  ```
  {
      "f1": "a",
      "f2": "b"
  }
  ```
  The function call:
  ```
  array($.f1, $.f2)
  ```
  Results: `["a", "b"]`
  */
  class ArrayFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.variable("array", 0, "zero or more expressions");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.isEmpty()) {
        return new ArrayFleakData();
      }

      List<FleakData> values =
          args.stream().map(arg -> visitExpression(visitor, arg)).collect(Collectors.toList());
      return new ArrayFleakData(values);
    }
  }

  /*
  strSplitFunction:
  Split a string into an array of substrings based on a delimiter.
  Syntax:
  ```
  str_split(string, delimiter)
  ```
  */
  class StrSplitFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("str_split", 2, "string and delimiter");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 2) {
        throw new IllegalArgumentException("str_split expects 2 arguments");
      }

      FleakData stringData = visitExpression(visitor, args.get(0));
      FleakData delimiterData = visitExpression(visitor, args.get(1));

      Preconditions.checkArgument(
          stringData instanceof StringPrimitiveFleakData,
          "str_split: first argument must be a string but found: %s",
          stringData != null ? stringData.unwrap() : null);

      Preconditions.checkArgument(
          delimiterData instanceof StringPrimitiveFleakData,
          "str_split: second argument (delimiter) must be a string but found: %s",
          delimiterData != null ? delimiterData.unwrap() : null);

      String inputString = stringData.getStringValue();
      String delimiter = delimiterData.getStringValue();

      if (inputString == null || inputString.isEmpty()) {
        return new ArrayFleakData(List.of());
      }

      String[] parts = inputString.split(Pattern.quote(delimiter));
      List<FleakData> resultList =
          Arrays.stream(parts).map(StringPrimitiveFleakData::new).collect(Collectors.toList());

      return new ArrayFleakData(resultList);
    }
  }

  /*
  substrFunction:
  Extract a substring from a string using SQL or Python style syntax.

  Supports multiple overloads:
  1. substr(string, start) - Extract from start position to end of string
     - Positive start: 0-based index from beginning
     - Negative start: Index from end (-1 is last character)

  2. substr(string, start, length) - SQL style: Extract 'length' characters starting at 'start'
     - Positive start: 0-based index from beginning
     - Negative start: Index from end
     - length: Number of characters to extract

  Edge cases:
  - Start index out of bounds:
    * start > string length: Returns empty string ""
    * start < -string length: Treated as 0 (beginning of string)
  - Length exceeds remaining characters: Returns substring up to end of string
  - Invalid argument types (non-integer start/length): Throws error
  - Null string input: Throws error
  - Negative length: Throws error

  Examples:
  - substr("hello", 1) returns "ello" (from index 1 to end)
  - substr("hello", -2) returns "lo" (last 2 characters)
  - substr("hello", 1, 2) returns "el" (2 characters starting at index 1)
  - substr("hello", -3, 2) returns "ll" (2 characters starting 3 from end)
  - substr("hello", 10) returns "" (start beyond string length)
  - substr("hello", -10) returns "hello" (negative start clamped to 0)
  - substr("hello", 2, 100) returns "llo" (length truncated to available characters)
  */
  class SubstrFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.optional(
          "substr", 2, 3, "string, start position, and optional length");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() < 2 || args.size() > 3) {
        throw new IllegalArgumentException("substr expects 2 or 3 arguments");
      }

      FleakData strFd = visitExpression(visitor, args.get(0));
      String str = strFd.getStringValue();

      // Use canonical integer evaluation method from visitor
      int start = visitor.evalArgAsInt(args.get(1), "start");
      int length = Integer.MAX_VALUE;

      if (args.size() == 3) {
        length = visitor.evalArgAsInt(args.get(2), "length");
      }

      String result = subStr(str, start, length);
      return FleakData.wrap(result);
    }

    private String subStr(String str, int start, int length) {
      if (str == null) {
        throw new IllegalArgumentException("Input string cannot be null");
      }
      if (length < 0) {
        throw new IllegalArgumentException("Length cannot be negative");
      }

      int strLen = str.length();

      if (start < 0) {
        start = strLen + start;
        if (start < 0) {
          start = 0;
        }
      }

      if (start >= strLen) {
        return "";
      }

      int endPos;
      if (length == Integer.MAX_VALUE || start > Integer.MAX_VALUE - length) {
        endPos = strLen;
      } else {
        endPos = start + length;
        if (endPos > strLen) {
          endPos = strLen;
        }
      }

      return str.substring(start, endPos);
    }
  }

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
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 1) {
        throw new IllegalArgumentException("duration_str_to_mills expects 1 argument");
      }

      FleakData durStrFd = visitExpression(visitor, args.get(0));
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
            String.format(
                "Invalid duration format: %s . Expected format is hh:mm:ss", durationStr));
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

  /*
  epochToTsStrFunction:
  Convert an epoch millisecond timestamp into a human readable string.
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
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 2) {
        throw new IllegalArgumentException("epoch_to_ts_str expects 2 arguments");
      }

      FleakData epochFd = visitExpression(visitor, args.get(0));
      Preconditions.checkArgument(
          epochFd instanceof NumberPrimitiveFleakData,
          "epoch_to_ts_str: timestamp field to be parsed is not a number: %s",
          epochFd);

      long epoch = (long) epochFd.getNumberValue();
      String patternStr = normalizeStrLiteral(args.get(1).getText());

      SimpleDateFormat simpleDateFormat;
      try {
        simpleDateFormat = new SimpleDateFormat(patternStr);
      } catch (Exception e) {
        throw new RuntimeException(
            "epoch_to_ts_str: failed to process date time pattern: " + patternStr);
      }

      String tsStr = simpleDateFormat.format(new Date(epoch));
      return new StringPrimitiveFleakData(tsStr);
    }
  }

  /*
  arr_flatten:
  Transforms an array containing nested arrays into an array by extracting all
  immediate elements from the first level of nesting only. This function performs a shallow
  flatten operation, not a deep recursive flatten.

  Syntax:
  ```
  arr_flatten(arr_of_arr)
  ```

  Parameters:
  - arr_of_arr: An array that may contain both simple elements and nested arrays at its first level.

  Return Value:
  - A new array with only one level of nesting removed. Deeper nested arrays remain intact.

  Behavior:
  - Extracts elements from first-level nested arrays only
  - Does not recursively flatten deeper nested arrays
  - Preserves the original order of elements
  - Non-array elements are included as-is in the result

  For example, given the input event:
  ```
  {
    "f": [
      [1, [2, 3]],
      [4, 5],
      6
    ]
  }
  ```

  When applying:
  ```
  arr_flatten($.f)
  ```

  Results in:
  ```
  [1, [2, 3], 4, 5, 6]
  ```
  Note that the inner array [2, 3] remains intact as the function only flattens one level.
  */
  class ArrFlattenFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("arr_flatten", 1, "array to flatten");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 1) {
        throw new IllegalArgumentException("arr_flatten expects 1 argument");
      }

      FleakData fleakData = visitExpression(visitor, args.get(0));
      Preconditions.checkArgument(
          fleakData instanceof ArrayFleakData,
          "arr_flatten argument must be an array but found: %s",
          fleakData.unwrap());

      List<FleakData> arrPayload =
          fleakData.getArrayPayload().stream()
              .peek(
                  l ->
                      Preconditions.checkArgument(
                          l instanceof ArrayFleakData,
                          "arr_flatten encountered non array data: %s",
                          l.unwrap()))
              .flatMap(l -> l.getArrayPayload().stream())
              .collect(Collectors.toList());

      return new ArrayFleakData(arrPayload);
    }
  }

  /*
  rangeFunction:
  Generates an array of integer numbers based on the provided arguments.
  The 'end' parameter in all variations is exclusive (the range goes up to,
  but does not include, 'end').

  Syntax and Behavior:

  1. range(count)
     - Generates integers from 0 up to (but not including) 'count', with a step of 1.
     - Equivalent to range(0, count, 1).
     - Example: range(5) produces [0, 1, 2, 3, 4].
     - If 'count' is 0 or negative, an empty array is generated.
     - Example: range(-2) produces [].

  2. range(start, end)
     - Generates integers from 'start' up to (but not including) 'end', with a step of 1.
     - Equivalent to range(start, end, 1).
     - Example: range(2, 5) produces [2, 3, 4].
     - If 'start' is greater than or equal to 'end', an empty array is generated.
     - Example: range(5, 2) produces [].

  3. range(start, end, step)
     - Generates integers starting from 'start', incrementing by 'step', and stopping
       before reaching 'end'.
     - 'step' can be positive (to count up) or negative (to count down).
     - 'step' cannot be zero.
     - If 'step' is positive: numbers 'x' are generated as long as 'x < end'.
       Example: range(0, 10, 2) produces [0, 2, 4, 6, 8].
     - If 'step' is negative: numbers 'x' are generated as long as 'x > end'.
       Example: range(10, 0, -2) produces [10, 8, 6, 4, 2].
     - An empty array is generated if the conditions for generation are not met
       from the start (e.g., if 'start >= end' with a positive 'step', or
       if 'start <= end' with a negative 'step').
       Example: range(0, 5, -1) produces [].
       Example: range(5, 0, 1) produces [].
  */
  class RangeFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.optional("range", 1, 3, "count, or start-end, or start-end-step");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.isEmpty() || args.size() > 3) {
        throw new IllegalArgumentException("range expects 1, 2, or 3 arguments");
      }

      int start = 0;
      int end;
      int step = 1;

      if (args.size() == 1) {
        // range(count)
        end = visitor.evalArgAsInt(args.get(0), "count");
      } else if (args.size() == 2) {
        // range(start, end)
        start = visitor.evalArgAsInt(args.get(0), "start");
        end = visitor.evalArgAsInt(args.get(1), "end");
      } else {
        // range(start, end, step)
        start = visitor.evalArgAsInt(args.get(0), "start");
        end = visitor.evalArgAsInt(args.get(1), "end");
        step = visitor.evalArgAsInt(args.get(2), "step");
      }

      if (step == 0) {
        throw new IllegalArgumentException("range() step argument cannot be zero.");
      }

      List<FleakData> resultNumbers = new ArrayList<>();
      if (step > 0) {
        for (long i = start; i < end; i += step) {
          resultNumbers.add(
              new NumberPrimitiveFleakData(i, NumberPrimitiveFleakData.NumberType.LONG));
        }
      } else {
        for (long i = start; i > end; i += step) {
          resultNumbers.add(
              new NumberPrimitiveFleakData(i, NumberPrimitiveFleakData.NumberType.LONG));
        }
      }

      return new ArrayFleakData(resultNumbers);
    }
  }

  /*
  arrForEachFunction:
  Apply the same logic for every element in a given array.
  Returns a new array containing the transformed elements.
  Syntax:
  ```
  arr_foreach($.path.to.array, variable_name, expression)
  ```
  where
  - `$.path.to.array` points to the array field in the input event.
  - If `$.path.to.array` points to an object, treat that object as a single element array
  - declares a variable name that represents each array element
  - the expression logic to be applied on every array element. use `variable_name` to refer to the array element

  For example:
  Given the input event:
  ```
  {
    "integration": "snmp",
    "attachments": {
      "snmp_pdf": "s3://a.pdf",
      "f1": "s3://b.pdf"
    },
    "resp": {
      "Test1": [
        {
          "operation_system": "windows",
          "ipAddr": "1.2.3.4"
        },
        {
          "operation_system": "windows",
          "ipAddr": "1.2.3.5"
        }
      ]
    }
  }

  ```

  and the `arr_foreach` expression:
  ```
  arr_foreach(
      $.resp.Test1,
      elem,
      dict(
          osVersion=elem.operation_system,
          source=$.integration,
          pdf_attachment=$.attachments.snmp_pdf,
          ip=elem.ipAddr
      )
  )
  ```

  Results:
  ```
  [
      {
        "source": "snmp",
        "osVersion": "windows",
        "pdf_attachment": "s3://a.pdf",
        "ip": "1.2.3.4"
      },
      {
        "source": "snmp",
        "osVersion": "windows",
        "pdf_attachment": "s3://a.pdf",
        "ip": "1.2.3.5"
      }
  ]
  ```
  */
  class ArrForEachFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("arr_foreach", 3, "array, variable name, and expression");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 3) {
        throw new IllegalArgumentException(
            "arr_foreach expects 3 arguments: array, variable name, and expression");
      }

      FleakData arrayData = visitExpression(visitor, args.get(0));
      if (!(arrayData instanceof ArrayFleakData) && !(arrayData instanceof RecordFleakData)) {
        throw new RuntimeException(
            "arr_foreach: first argument should be an array or object but found: "
                + arrayData.getClass().getSimpleName());
      }

      if (arrayData instanceof RecordFleakData) {
        arrayData = new ArrayFleakData(List.of(arrayData));
      }

      // Second argument should be a variable name (IDENTIFIER)
      String elemVarName = args.get(1).getText();

      List<FleakData> resultArray = new ArrayList<>();
      for (FleakData elem : arrayData.getArrayPayload()) {
        // Create new scope, set variable, evaluate expression, then exit scope
        visitor.enterScope();
        try {
          visitor.setVariable(elemVarName, elem);
          FleakData resultElem = visitExpression(visitor, args.get(2));
          resultArray.add(resultElem);
        } finally {
          visitor.exitScope();
        }
      }

      return new ArrayFleakData(resultArray);
    }
  }

  /*
  dictMergeFunction:
  Merge a set of dictionaries into one.
  For example:
  ```
  dict_merge(
    $,
    grok($.__raw__, "%{GREEDYDATA:timestamp} %{HOSTNAME:hostname} %{WORD:program}\\[%{POSINT:pid}\\]: %ASA-%{INT:level}-%{INT:message_number}: %{GREEDYDATA:message_text}")
  )
  ```
  (`$` refers to the current input event)

  It's required that all arguments are dictionaries.
  For every argument, add all its key value pairs to the result dictionary. Duplicated keys are overriden.

  returns a merged dict
  */
  class DictMergeFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.variable("dict_merge", 1, "one or more dictionaries");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.isEmpty()) {
        throw new IllegalArgumentException("dict_merge expects at least 1 argument");
      }

      Map<String, FleakData> mergedPayload = new HashMap<>();
      for (EvalExpressionParser.ExpressionContext arg : args) {
        FleakData fd = visitExpression(visitor, arg);
        if (fd == null) {
          continue; // Skip null values
        }
        Preconditions.checkArgument(
            fd instanceof RecordFleakData,
            "dict_merge: every argument must be a record but found: %s",
            fd.unwrap());
        mergedPayload.putAll(fd.getPayload());
      }

      return new RecordFleakData(mergedPayload);
    }
  }

  /*
  floorFunction:
  Round down a floating point number to the nearest integer.
  Example: floor(123.45) => 123, floor(-123.45) => -124
  */
  class FloorFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("floor", 1, "number to round down");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 1) {
        throw new IllegalArgumentException("floor expects 1 argument");
      }

      FleakData arg = visitExpression(visitor, args.get(0));
      if (!(arg instanceof NumberPrimitiveFleakData)) {
        throw new IllegalArgumentException(
            "floor: argument must be a number but found: "
                + (arg != null ? arg.getClass().getSimpleName() : "null"));
      }

      double value = arg.getNumberValue();
      long floorValue = (long) Math.floor(value);
      return new NumberPrimitiveFleakData(floorValue, NumberPrimitiveFleakData.NumberType.LONG);
    }
  }

  /*
  ceilFunction:
  Round up a floating point number to the nearest integer.
  Example: ceil(123.45) => 124, ceil(-123.45) => -123
  */
  class CeilFunction implements FeelFunction {
    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.required("ceil", 1, "number to round up");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      if (args.size() != 1) {
        throw new IllegalArgumentException("ceil expects 1 argument");
      }

      FleakData arg = visitExpression(visitor, args.get(0));
      if (!(arg instanceof NumberPrimitiveFleakData)) {
        throw new IllegalArgumentException(
            "ceil: argument must be a number but found: "
                + (arg != null ? arg.getClass().getSimpleName() : "null"));
      }

      double value = arg.getNumberValue();
      long ceilValue = (long) Math.ceil(value);
      return new NumberPrimitiveFleakData(ceilValue, NumberPrimitiveFleakData.NumberType.LONG);
    }
  }

  /*
  pythonFunction:
  Execute a single Python function automatically discovered within the script string.

  Syntax:
  python("<python_script_with_one_function_def>", arg1, arg2, ...)

  - The first argument MUST be a string literal containing the Python script.
    This script MUST define exactly one top-level function usable as the entry point.
  - Subsequent arguments (arg1, arg2, ...) are standard FEEL expressions whose
    evaluated values will be passed to the discovered Python function.
  - Returns the value returned by the Python function, converted back to FEEL data types.
  - Throws an error if zero or more than one function is found in the script.
  - Requires GraalVM with Python language support configured.
  */
  @Slf4j
  record PythonFunction(PythonExecutor pythonExecutor) implements FeelFunction {

    @Override
    public FunctionSignature getSignature() {
      return FunctionSignature.variable("python", 1, "script string and optional arguments");
    }

    @Override
    public FleakData evaluate(
        ExpressionValueVisitor visitor, List<EvalExpressionParser.ExpressionContext> args) {
      throw new UnsupportedOperationException(
          "Python function requires context for proper script matching. "
              + "This should be called via evaluateWithContext() method.");
    }

    public FleakData evaluateWithContext(
        ExpressionValueVisitor visitor,
        List<EvalExpressionParser.ExpressionContext> args,
        EvalExpressionParser.GenericFunctionCallContext ctx) {
      if (pythonExecutor == null) {
        throw new IllegalArgumentException(
            "cannot execute python() function. No python executor provided");
      }

      if (args.isEmpty()) {
        throw new IllegalArgumentException("python function expects at least 1 argument (script)");
      }

      // Use the context to lookup the pre-compiled function
      CompiledPythonFunction compiledFunc = pythonExecutor.getCompiledPythonFunctions().get(ctx);

      if (compiledFunc == null) {
        throw new IllegalStateException(
            "No pre-compiled Python function found for context: "
                + ctx.getSourceInterval()
                + ". Ensure Python functions are pre-compiled before execution.");
      }

      // Evaluate the arguments (skip first argument which is the script)
      List<FleakData> feelArgs =
          args.subList(1, args.size()).stream()
              .map(arg -> visitExpression(visitor, arg))
              .collect(Collectors.toList());

      return executePythonFunction(compiledFunc, feelArgs);
    }

    // Note: Removed findAnyCompiledFunction() as it was dangerous -
    // would execute wrong Python script if multiple scripts exist

    private FleakData executePythonFunction(
        CompiledPythonFunction compiledFunc, List<FleakData> feelArgs) {
      Value targetFunction = compiledFunc.functionValue();

      try (Context context = compiledFunc.pythonContext()) {
        // Convert FEEL arguments to Python arguments
        Object[] pythonArgs =
            feelArgs.stream()
                .map(fd -> fd != null ? fd.unwrap() : null)
                .map(
                    o -> {
                      if (o instanceof List<?> list) {
                        Value pythonList = context.eval("python", "[]");
                        for (Object e : list) {
                          pythonList.invokeMember("append", context.asValue(e));
                        }
                        return pythonList;
                      } else {
                        return o;
                      }
                    })
                .toArray();
        context.enter();
        try {
          Value pyResult = targetFunction.execute(pythonArgs);
          return graalValueToFleakData(pyResult);
        } finally {
          context.leave();
        }
      } catch (PolyglotException e) {
        throw new IllegalArgumentException(
            "Error during execution of Python function: " + e.getMessage(), e);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "An unexpected error occurred executing Python function: " + e.getMessage(), e);
      }
    }
  }

  // Helper method to create FUNCTIONS_TABLE with optional PythonExecutor
  static Map<String, FeelFunction> createFunctionsTable(PythonExecutor pythonExecutor) {
    var builder =
        ImmutableMap.<String, FeelFunction>builder()
            .put("ts_str_to_epoch", new TsStrToEpochFunction())
            .put("epoch_to_ts_str", new EpochToTsStrFunction())
            .put("str_contains", new StrContainsFunction())
            .put("to_str", new ToStringFunction())
            .put("upper", new UpperFunction())
            .put("lower", new LowerFunction())
            .put("size_of", new SizeOfFunction())
            .put("grok", new GrokFunction())
            .put("parse_int", new ParseIntFunction())
            .put("parse_float", new ParseFloatFunction())
            .put("array", new ArrayFunction())
            .put("str_split", new StrSplitFunction())
            .put("substr", new SubstrFunction())
            .put("duration_str_to_mills", new DurationStrToMillsFunction())
            .put("arr_flatten", new ArrFlattenFunction())
            .put("range", new RangeFunction())
            .put("arr_foreach", new ArrForEachFunction())
            .put("dict_merge", new DictMergeFunction())
            .put("floor", new FloorFunction())
            .put("ceil", new CeilFunction());

    if (pythonExecutor != null) {
      builder.put("python", new PythonFunction(pythonExecutor));
    }

    return builder.build();
  }

  // Note: FUNCTIONS_TABLE is now created at runtime in ExpressionValueVisitor
  // to properly support runtime dependencies like PythonExecutor
}
