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
package io.fleak.zephflow.lib.pathselect;

import static io.fleak.zephflow.lib.utils.AntlrUtils.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.ExpressionValueVisitor;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Created by bolei on 5/23/24 */
@Data
@NoArgsConstructor
public class PathExpression {

  private EvalExpressionParser.PathSelectExprContext pathSelectExprContext;

  public PathExpression(EvalExpressionParser.PathSelectExprContext pathSelectExprContext) {
    this.pathSelectExprContext = pathSelectExprContext;
  }

  public static PathExpression fromString(String jsonPathString) {
    try {
      return fromStringOrThrow(jsonPathString);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "failed to parse path expression: {%s}. Error message: %s",
              jsonPathString, e.getMessage()),
          e);
    }
  }

  private static PathExpression fromStringOrThrow(String jsonPathString) {
    EvalExpressionParser expressionParser =
        (EvalExpressionParser) parseInput(jsonPathString, GrammarType.EVAL);
    EvalExpressionParser.PathSelectExprContext ctx = expressionParser.pathSelectExpr();
    ensureConsumedAllTokens(expressionParser);

    return new PathExpression(ctx);
  }

  public FleakData calculateValue(FleakData input) {
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(input, null);
    return visitor.visit(pathSelectExprContext);
  }

  @Override
  public String toString() {
    return pathSelectExprContext.getText();
  }

  @Override
  public int hashCode() {
    return pathSelectExprContext.getText().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PathExpression that) {
      return Objects.equals(pathSelectExprContext.getText(), that.pathSelectExprContext.getText());
    }
    return false;
  }

  public String getStringValueFromEventOrDefault(FleakData inputEvent, String defaultValue) {
    return getValueFromEvent(
        inputEvent, new ValueExtractor.StringValueExtractor(defaultValue, null));
  }

  @SuppressWarnings("unused")
  public String getStringValueFromEventOrThrow(
      FleakData inputEvent, String errorMessageTemplate, Object... args) {
    return getValueFromEvent(
        inputEvent,
        new ValueExtractor.StringValueExtractor(
            null, new RuntimeExceptionSupplier(errorMessageTemplate, args)));
  }

  @SuppressWarnings("unused")
  public List<Float> getFloatArrayFromEventOrThrow(
      FleakData inputEvent, String errorMessageTemplate, Object... args) {
    Supplier<RuntimeException> exceptionSupplier =
        new RuntimeExceptionSupplier(errorMessageTemplate, args);

    ValueExtractor.ArrayValueExtractor<Float> arrayValueExtractor =
        new ValueExtractor.ArrayValueExtractor<>(
            null,
            exceptionSupplier,
            new ValueExtractor.FloatValueExtractor(null, exceptionSupplier));
    return getValueFromEvent(inputEvent, arrayValueExtractor);
  }

  private <T> T getValueFromEvent(FleakData inputEvent, ValueExtractor<T> extractor) {
    FleakData value;
    try {
      value = calculateValue(inputEvent);
    } catch (Exception e) {
      return extractor.handleError();
    }
    if (value == null) {
      return extractor.handleError();
    }
    return extractor.extractValue(value);
  }

  public record RuntimeExceptionSupplier(String errorMessageTemplate, Object[] args)
      implements Supplier<RuntimeException> {

    @Override
    public RuntimeException get() {
      String errorMsg = String.format(errorMessageTemplate, args);
      return new RuntimeException(errorMsg);
    }
  }
}
