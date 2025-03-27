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
package io.fleak.zephflow.lib.utils;

import static io.fleak.zephflow.lib.utils.AntlrUtils.GrammarType.*;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import io.fleak.zephflow.lib.antlr.*;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import lombok.*;
import org.antlr.v4.runtime.*;

/** Created by bolei on 5/23/24 */
public interface AntlrUtils {
  Map<GrammarType, Function<String, Lexer>> LEXTER_MAP =
      Map.of(
          EXPRESSION_SQL,
          input -> new ExpressionSQLParserLexer(CharStreams.fromString(input)),
          EVAL,
          input -> new EvalExpressionLexer(CharStreams.fromString(input)));

  Map<GrammarType, Function<CommonTokenStream, Parser>> PARSER_MAP =
      Map.of(
          EXPRESSION_SQL, ExpressionSQLParserParser::new,
          EVAL, EvalExpressionParser::new);

  static void ensureConsumedAllTokens(Parser parser) {

    // Get the current token (where the parser stopped)
    Token currentToken = parser.getCurrentToken();
    if (currentToken.getType() != Token.EOF) {
      throw new IllegalArgumentException(
          "Encountered parsing error at position " + currentToken.getStopIndex());
    }
  }

  static void validateEvalExpression(String evalExpr) {
    try {
      EvalExpressionParser parser = (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, EVAL);
      parser.language();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "failed to parse eval expression: %s. reason: %s", evalExpr, e.getMessage()));
    }
  }

  static String cleanIdentifier(String v) {
    return cleanWrappedString(v, '\"');
  }

  static String cleanStringLiteral(String v) {
    return cleanWrappedString(v, '\'');
  }

  static String cleanWrappedString(String v, char wrappedChar) {
    int len = v.length();
    if (len > 0) {
      int from = 0;
      int to = len;

      if (v.charAt(0) == wrappedChar) from = 1;
      if (v.charAt(len - 1) == wrappedChar) to -= 1;
      if (from > 0 || to < len) return v.substring(from, to);
    }

    return v;
  }

  static Parser parseInput(String input, GrammarType grammarType) {
    var lexer = Objects.requireNonNull(AntlrUtils.LEXTER_MAP.get(grammarType)).apply(input);
    lexer.removeErrorListeners();
    lexer.addErrorListener(ParseErrorListener.INSTANCE);
    var tokens = new CommonTokenStream(lexer);
    var parser = Objects.requireNonNull(AntlrUtils.PARSER_MAP.get(grammarType)).apply(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(ParseErrorListener.INSTANCE);
    return parser;
  }

  enum GrammarType {
    EXPRESSION_SQL,
    EVAL
  }

  class ParseErrorListener extends BaseErrorListener {
    public static final ParseErrorListener INSTANCE = new ParseErrorListener();

    @Override
    public void syntaxError(
        Recognizer<?, ?> recognizer,
        Object offendingSymbol,
        int line,
        int charPositionInLine,
        String msg,
        RecognitionException e) {

      ParseErrorDto errorDto =
          ParseErrorDto.builder()
              .offendingSymbol(Objects.toString(offendingSymbol))
              .line(line)
              .charPositionInLine(charPositionInLine)
              .message(msg)
              .build();

      throw new FleakParseException(errorDto);
    }
  }

  @Getter
  class FleakParseException extends RuntimeException {
    private final ParseErrorDto parseErrorDto;

    FleakParseException(ParseErrorDto parseErrorDto) {
      super(toJsonString(parseErrorDto));
      this.parseErrorDto = parseErrorDto;
    }
  }

  @Data
  @Builder
  @AllArgsConstructor
  @NoArgsConstructor
  class ParseErrorDto {
    private String offendingSymbol;
    private int line;
    private int charPositionInLine;
    private String message;
  }
}
