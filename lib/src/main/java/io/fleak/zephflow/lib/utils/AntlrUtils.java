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
import io.fleak.zephflow.lib.commands.eval.FeelFunction;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
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

  Map<String, FeelFunction.FunctionSignature> FEEL_FUNCTION_SIGNATURES =
      createAllFunctionSignatures();

  private static Map<String, FeelFunction.FunctionSignature> createAllFunctionSignatures() {
    // Create signatures from base functions (without PythonExecutor)
    Map<String, FeelFunction.FunctionSignature> signatures =
        FeelFunction.createFunctionsTable(null).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSignature()));

    // Add Python function signature by creating a dummy instance to get its signature
    signatures.put("python", new FeelFunction.PythonFunction(null).getSignature());

    return signatures;
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

      String enhancedMessage = enhanceErrorMessage(recognizer, offendingSymbol, msg);

      ParseErrorDto errorDto =
          ParseErrorDto.builder()
              .offendingSymbol(Objects.toString(offendingSymbol))
              .line(line)
              .charPositionInLine(charPositionInLine)
              .message(enhancedMessage)
              .build();

      throw new FleakParseException(errorDto);
    }

    private String enhanceErrorMessage(
        Recognizer<?, ?> recognizer, Object offendingSymbol, String originalMessage) {
      if (!(recognizer instanceof Parser parser)) {
        return originalMessage;
      }

      // Try to detect function argument errors
      String functionErrorMessage =
          detectFunctionArgumentError(parser, offendingSymbol, originalMessage);
      if (functionErrorMessage != null) {
        return functionErrorMessage;
      }

      return originalMessage;
    }

    private String detectFunctionArgumentError(
        Parser parser, Object offendingSymbol, String originalMessage) {
      if (!originalMessage.contains("no viable alternative")
          && !originalMessage.contains("mismatched input")
          && !originalMessage.contains("expecting")) {
        return null;
      }

      try {
        var context = parser.getContext();
        while (context != null) {
          if (context
              instanceof
              EvalExpressionParser.GenericFunctionCallContext genericFunctionCallContext) {
            String functionName = genericFunctionCallContext.IDENTIFIER().getText();
            return createFunctionArgumentErrorMessage(
                functionName, offendingSymbol, originalMessage);
          }
          context = context.getParent();
        }
      } catch (Exception ex) {
        return null;
      }

      return null;
    }

    private String createFunctionArgumentErrorMessage(
        String functionName, Object offendingSymbol, String originalMessage) {
      FeelFunction.FunctionSignature signature = FEEL_FUNCTION_SIGNATURES.get(functionName);
      if (signature == null) {
        return originalMessage;
      }

      String symbolText = offendingSymbol != null ? offendingSymbol.toString() : "";

      if (symbolText.contains("')'")
          && (originalMessage.contains("no viable alternative")
              || originalMessage.contains("expecting ','")
              || originalMessage.contains("mismatched input ')'"))) {

        if (signature.maxArgs() == signature.minArgs()) {
          return String.format(
              "Function '%s' expects %d argument(s) (%s)",
              functionName, signature.minArgs(), signature.description());
        } else if (signature.maxArgs() == -1) {
          return String.format(
              "Function '%s' expects at least %d argument(s) (%s)",
              functionName, signature.minArgs(), signature.description());
        } else {
          return String.format(
              "Function '%s' expects %d-%d argument(s) (%s)",
              functionName, signature.minArgs(), signature.maxArgs(), signature.description());
        }
      }

      return String.format("Error in function '%s' - %s", functionName, originalMessage);
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
