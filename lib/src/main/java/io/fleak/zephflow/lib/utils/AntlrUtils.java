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

import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.lib.antlr.*;
import io.fleak.zephflow.lib.commands.eval.FeelFunction;
import io.fleak.zephflow.lib.commands.eval.FunctionSignature;
import io.fleak.zephflow.lib.commands.eval.PythonFunction;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.*;
import org.antlr.v4.runtime.*;

/** Created by bolei on 5/23/24 */
public interface AntlrUtils {
  Map<GrammarType, Function<String, Lexer>> LEXTER_MAP =
      Map.of(EVAL, input -> new EvalExpressionLexer(CharStreams.fromString(input)));

  Map<GrammarType, Function<CommonTokenStream, Parser>> PARSER_MAP =
      Map.of(EVAL, EvalExpressionParser::new);

  static void ensureConsumedAllTokens(Parser parser) {

    // Get the current token (where the parser stopped)
    Token currentToken = parser.getCurrentToken();
    if (currentToken.getType() != Token.EOF) {
      throw ConfigurationValidationException.at(
          ConfigurationPath.root(),
          new IllegalArgumentException(
              "Encountered parsing error at position " + currentToken.getStopIndex()),
          "Unexpected trailing input at line "
              + currentToken.getLine()
              + ", column "
              + currentToken.getCharPositionInLine()
              + "; expected end of expression.");
    }
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

  /** Adds a caller's configuration location to a recognized syntax or compilation failure. */
  static RuntimeException configurationFailure(
      ConfigurationPath path, Exception failure, RuntimeException originalFailure) {
    if (failure instanceof FleakParseException syntax) {
      return new ConfigurationValidationException(
          originalFailure, List.of(syntax.getConfigurationIssue().withPrefix(path)));
    }
    if (failure instanceof ConfigurationValidationException configuration) {
      return new ConfigurationValidationException(
          originalFailure,
          configuration.issues().stream().map(issue -> issue.withPrefix(path)).toList());
    }
    return originalFailure;
  }

  enum GrammarType {
    EVAL
  }

  Map<String, FunctionSignature> FEEL_FUNCTION_SIGNATURES = createAllFunctionSignatures();

  private static Map<String, FunctionSignature> createAllFunctionSignatures() {
    // Create signatures from base functions (without PythonExecutor)
    Map<String, FunctionSignature> signatures =
        FeelFunction.createFunctionsTable(null).entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSignature()));

    // Add Python function signature by creating a dummy instance to get its signature
    signatures.put("python", new PythonFunction(null).getSignature());

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

      throw new FleakParseException(
          errorDto, configurationIssue(recognizer, line, charPositionInLine));
    }

    private ConfigurationIssue configurationIssue(
        Recognizer<?, ?> recognizer, int line, int position) {
      String reason =
          recognizer instanceof Lexer
              ? "Invalid character or token in expression"
              : "Invalid expression syntax";
      String message = reason + " at line " + line + ", column " + position;
      if (recognizer instanceof Parser parser) {
        message += ". Expected: " + parser.getExpectedTokens().toString(parser.getVocabulary());
        var context = parser.getContext();
        while (context != null) {
          if (context instanceof EvalExpressionParser.GenericFunctionCallContext call
              && call.IDENTIFIER() != null) {
            String name = call.IDENTIFIER().getText();
            FunctionSignature signature = FEEL_FUNCTION_SIGNATURES.get(name);
            if (signature != null) {
              message +=
                  ". Function '"
                      + name
                      + "' expects "
                      + signature.minArgs()
                      + (signature.maxArgs() == signature.minArgs()
                          ? ""
                          : signature.maxArgs() == -1 ? " or more" : "-" + signature.maxArgs())
                      + " argument(s)";
            }
            break;
          }
          context = context.getParent();
        }
      }
      return new ConfigurationIssue(ConfigurationPath.root(), message);
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
      FunctionSignature signature = FEEL_FUNCTION_SIGNATURES.get(functionName);
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
    private final ConfigurationIssue configurationIssue;

    FleakParseException(ParseErrorDto parseErrorDto) {
      this(
          parseErrorDto,
          new ConfigurationIssue(
              ConfigurationPath.root(),
              "Invalid expression syntax at line "
                  + parseErrorDto.getLine()
                  + ", column "
                  + parseErrorDto.getCharPositionInLine()));
    }

    FleakParseException(ParseErrorDto parseErrorDto, ConfigurationIssue configurationIssue) {
      super(toJsonString(parseErrorDto));
      this.parseErrorDto = parseErrorDto;
      this.configurationIssue = configurationIssue;
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
