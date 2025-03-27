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

public class ErrorBeautifier {

  public static String beautifyRuntimeErrorMessage(String msg) {
    return ErrorParserHelper.betterNotSuportedMessage(
        ErrorParserHelper.removeJavaClassReferences(msg));
  }

  public static String beautifySyntaxErrorMessage(String sql, String msg) {
    StringBuilder builder = new StringBuilder();

    var isPositionError = ErrorParserHelper.parseLineAndPosition(msg);

    if (isPositionError.isPresent()) {
      builder.append(isPositionError.get().issue());
      builder.append("\n");
      builder.append(highlightError(sql, msg, isPositionError.get()));
    } else {
      builder.append(ErrorBeautifier.beautifyRuntimeErrorMessage(msg));
    }

    return builder.toString();
  }

  private static String highlightError(
      String sql, String msg, ErrorLineLocation errorLineLocation) {
    var lines = sql.lines().toArray(String[]::new);
    try {

      var line = lines[errorLineLocation.line()];
      var highlight = " ".repeat(Math.max(0, errorLineLocation.position() - 1)) + "^^^";
      return line + "\n" + highlight;

    } catch (IndexOutOfBoundsException e) {
      return msg;
    }
  }
}
