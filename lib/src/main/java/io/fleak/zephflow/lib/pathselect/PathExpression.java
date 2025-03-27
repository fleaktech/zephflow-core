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
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Created by bolei on 5/23/24 */
@Data
@NoArgsConstructor
public class PathExpression {

  private List<Step> path;

  public PathExpression(List<Step> path) {
    this.path = path;
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
    List<Step> steps =
        ctx.step().stream()
            .map(
                stepContext -> {
                  if (stepContext.fieldAccess() != null) { // field name
                    EvalExpressionParser.FieldAccessContext fieldAccessContext =
                        stepContext.fieldAccess();
                    return fieldAccessContext.IDENTIFIER() == null
                        ? new BracketFieldNameStep(
                            unescapeStrLiteral(fieldAccessContext.QUOTED_IDENTIFIER().getText()))
                        : new IdFieldNameStep(fieldAccessContext.IDENTIFIER().getText());
                  } else { // array access
                    String indexStr = stepContext.arrayAccess().INT_LITERAL().getText();
                    int index = Integer.parseInt(indexStr);
                    return new ArrayAccessStep(index);
                  }
                })
            .toList();
    List<Step> path = new ArrayList<>();
    path.add(new RootStep());
    path.addAll(steps);
    return new PathExpression(path);
  }

  public FleakData calculateValue(FleakData input) {
    return evaluateContent(
        input,
        0,
        (data, step) -> {
          if (data == null) {
            return null;
          }
          if (step instanceof RootStep) {
            return data;
          }
          if (step instanceof FieldNameStep) {
            return handleFieldNameStepValue(data, ((FieldNameStep) step).name());
          }

          ArrayAccessStep arrayAccessStep = ((ArrayAccessStep) step);
          if (!(data instanceof ArrayFleakData)) {
            return null;
          }
          List<FleakData> arrayPayload = data.getArrayPayload();
          int idx = arrayAccessStep.getIndex();
          if (idx >= arrayPayload.size()) {
            return null;
          }
          return data.getArrayPayload().get(idx);
        });
  }

  @Override
  public String toString() {
    return path.stream().map(Step::toString).collect(Collectors.joining());
  }

  private FleakData handleFieldNameStepValue(FleakData data, String fieldName) {
    if (!(data instanceof RecordFleakData)) {
      return null;
    }
    if (data.getPayload() == null) {
      return null;
    }
    return data.getPayload().get(fieldName);
  }

  private <T> T evaluateContent(T currentContent, int level, ValueCalculator<T> valueCalculator) {
    Preconditions.checkArgument(validArrayIndex(path, level));
    Step step = path.get(level);
    T value = valueCalculator.calculateValue(currentContent, step);
    if (level >= path.size() - 1) {
      return value;
    }
    return evaluateContent(value, level + 1, valueCalculator);
  }

  public String getStringValueFromEventOrDefault(FleakData inputEvent, String defaultValue) {
    return getValueFromEvent(
        inputEvent, new ValueExtractor.StringValueExtractor(defaultValue, null));
  }

  public String getStringValueFromEventOrThrow(
      FleakData inputEvent, String errorMessageTemplate, Object... args) {
    return getValueFromEvent(
        inputEvent,
        new ValueExtractor.StringValueExtractor(
            null, new RuntimeExceptionSupplier(errorMessageTemplate, args)));
  }

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

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "stepType")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = RootStep.class, name = "root"),
    @JsonSubTypes.Type(value = IdFieldNameStep.class, name = "fieldName"),
    @JsonSubTypes.Type(value = ArrayAccessStep.class, name = "index"),
  })
  public interface Step extends Serializable {}

  interface FieldNameStep extends Step {
    String name();
  }

  public interface ValueCalculator<T> {
    T calculateValue(T currentContent, Step s);
  }

  @Data
  public static class RootStep implements Step {
    public String toString() {
      return "$";
    }
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class IdFieldNameStep implements FieldNameStep {
    String name;

    @Override
    public String toString() {
      return "." + name;
    }

    @Override
    public String name() {
      return name;
    }
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class BracketFieldNameStep implements FieldNameStep {
    String name;

    @Override
    public String toString() {
      return "[" + escapeStrLiteral(name) + "]";
    }

    @Override
    public String name() {
      return name;
    }
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class ArrayAccessStep implements Step {
    int index;

    @Override
    public String toString() {
      return String.format("[%d]", index);
    }
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
