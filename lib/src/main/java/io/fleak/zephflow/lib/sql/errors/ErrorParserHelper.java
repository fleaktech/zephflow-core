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
package io.fleak.zephflow.lib.sql.errors;

import java.util.Optional;
import java.util.regex.Pattern;

public class ErrorParserHelper {

  public static Pattern AT_POSITION_PATTERN = Pattern.compile("(.*) at position (\\d+)");
  public static Pattern AT_LINE_AT_POSITION_PATTERN =
      Pattern.compile("(.*) at line (\\d+) position (\\d+): (.*)");

  public static Optional<ErrorLineLocation> parseLineAndPosition(String error) {
    return parseAtLinePosition(error).or(() -> parseAtPosition(error));
  }

  public static Optional<ErrorLineLocation> parseAtLinePosition(String error) {
    var matcher = AT_LINE_AT_POSITION_PATTERN.matcher(error);
    if (matcher.find()) {
      try {
        var errorMsg = matcher.group(4);

        int line = Integer.parseInt(matcher.group(2)) - 1; // support zero indexed lines
        int position = Integer.parseInt(matcher.group(3));
        return Optional.of(new ErrorLineLocation(line, position, errorMsg));
      } catch (NumberFormatException ignored) {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  /** Encountered parsing error at position 16 */
  public static Optional<ErrorLineLocation> parseAtPosition(String error) {
    var matcher = AT_POSITION_PATTERN.matcher(error);
    if (matcher.find()) {
      try {
        var errorMsg = matcher.group(1);
        int position = Integer.parseInt(matcher.group(2)) - 1; // support zero indexed lines
        return Optional.of(new ErrorLineLocation(0, position, errorMsg));
      } catch (NumberFormatException ignored) {
      }
    }
    return Optional.empty();
  }

  public static String betterNotSuportedMessage(String error) {
    if (error.contains("not supported")) {
      return "Features used in this query are not supported by our SQL Engine";
    }
    return error;
  }

  public static String removeJavaClassReferences(String error) {

    if (error == null) return "";

    var pattern = Pattern.compile("java\\.([a-zA-Z0-9_])\\.([_a-zA-Z0-9]+)");

    var matcher = pattern.matcher(error);
    return matcher.replaceAll(
        (m) ->
            switch (m.group(1)) {
              case "String" -> "text";
              case "Integer", "Long", "Short" -> "int";
              case "Double", "Float" -> "numeric";
              case "Boolean" -> "boolean";
              case "List", "ArrayList" -> "array";
              case "HashMap", "LinkedHashMap" -> "record";
              default -> m.group(1);
            });
  }
}
