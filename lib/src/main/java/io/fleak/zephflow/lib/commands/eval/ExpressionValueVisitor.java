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

import static io.fleak.zephflow.lib.commands.eval.EvaluatorFuncs.BINARY_VALUE_EVALUATOR_FUNC_MAP;
import static io.fleak.zephflow.lib.commands.eval.EvaluatorFuncs.UNARY_VALUE_EVALUATOR_FUNC_MAP;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.antlr.EvalExpressionBaseVisitor;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections4.CollectionUtils;

/** Created by bolei on 10/19/24 */
@Slf4j
public class ExpressionValueVisitor extends EvalExpressionBaseVisitor<FleakData> {

  private final LinkedList<Map<String, FleakData>> variableEnvironment;

  private final boolean lenient;

  private final Map<String, FeelFunction> functionsTable;

  private final ExpressionCache cache;

  private ExpressionValueVisitor(
      List<Map<String, FleakData>> variableEnvironment,
      boolean lenient,
      PythonExecutor pythonExecutor,
      ExpressionCache cache) {
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(variableEnvironment));
    this.variableEnvironment = new LinkedList<>(variableEnvironment);
    this.lenient = lenient;
    this.functionsTable = FeelFunction.createFunctionsTable(pythonExecutor);
    this.cache = cache;
  }

  public static ExpressionValueVisitor createInstance(
      FleakData fleakData, PythonExecutor pythonExecutor, ExpressionCache cache) {
    return createInstance(fleakData, false, pythonExecutor, cache);
  }

  public static ExpressionValueVisitor createInstance(
      FleakData fleakData, boolean lenient, PythonExecutor pythonExecutor, ExpressionCache cache) {
    List<Map<String, FleakData>> variableEnvironment =
        List.of(Map.of(ROOT_OBJECT_VARIABLE_NAME, fleakData));
    return new ExpressionValueVisitor(variableEnvironment, lenient, pythonExecutor, cache);
  }

  @Override
  public FleakData visit(ParseTree tree) {
    return super.visit(tree);
  }

  @Override
  public FleakData visitLanguage(EvalExpressionParser.LanguageContext ctx) {
    return visit(ctx.expression());
  }

  @Override
  public FleakData visitExpression(EvalExpressionParser.ExpressionContext ctx) {
    return visit(ctx.conditionalOr());
  }

  @Override
  public FleakData visitConditionalOr(EvalExpressionParser.ConditionalOrContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitConditionalAnd(EvalExpressionParser.ConditionalAndContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitEquality(EvalExpressionParser.EqualityContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitRelational(EvalExpressionParser.RelationalContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitAdditive(EvalExpressionParser.AdditiveContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitMultiplicative(EvalExpressionParser.MultiplicativeContext ctx) {
    return visitBinaryNode(ctx);
  }

  @Override
  public FleakData visitUnary(EvalExpressionParser.UnaryContext ctx) {
    Preconditions.checkArgument(ctx.getChildCount() == 1 || ctx.getChildCount() == 2);
    // visit the last child to get the value
    if (ctx.getChildCount() == 1) {
      return visit(ctx.getChild(0));
    }
    FleakData val = visit(ctx.getChild(1));
    ParseTree opNode = ctx.getChild(0);
    // Use cached operator text if available
    String op = cache != null ? cache.getOperator(opNode) : opNode.getText();
    EvaluatorFuncs.UnaryEvaluatorFunc<FleakData> unaryEvaluator =
        Objects.requireNonNull(UNARY_VALUE_EVALUATOR_FUNC_MAP.get(op));
    return unaryEvaluator.evaluate(val);
  }

  @Override
  public FleakData visitPrimary(EvalExpressionParser.PrimaryContext ctx) {
    // First, evaluate the main part of the primary expression
    FleakData value;

    if (ctx.getChildCount() >= 3
        && "(".equals(ctx.getChild(0).getText())
        && ")".equals(ctx.getChild(2).getText())) {
      value = visit(ctx.getChild(1)); // '(' expression ')'
    } else if (ctx.IDENTIFIER() != null) {
      // First alternative: IDENTIFIER (step)*
      String varName = ctx.IDENTIFIER().getText();
      // Retrieve the variable from the current context/environment
      value = getVariableValue(varName);
    } else if (ctx.pathSelectExpr() != null) {
      // Second alternative: pathSelectExpr
      value = visit(ctx.pathSelectExpr());
    } else if (ctx.genericFunctionCall() != null) {
      value = visit(ctx.genericFunctionCall());
    } else if (ctx.value() != null) {
      value = visit(ctx.value());
    } else if (ctx.caseExpression() != null) {
      value = visit(ctx.caseExpression());
    } else if (ctx.dictExpression() != null) {
      value = visit(ctx.dictExpression());
    } else {
      throw new IllegalStateException("Invalid primary reference");
    }

    // Apply any steps if present (like field access: elem.operation_system)
    if (ctx.step() != null && !ctx.step().isEmpty()) {
      for (EvalExpressionParser.StepContext stepCtx : ctx.step()) {
        value = applyStep(value, stepCtx);
        if (value == null) {
          return null;
        }
      }
    }

    return value;
  }

  @Override
  public FleakData visitValue(EvalExpressionParser.ValueContext ctx) {
    TerminalNode tn = ctx.QUOTED_IDENTIFIER();
    if (tn != null) {
      // Use cached normalized string if available
      String normalized = cache != null ? cache.getNormalizedString(tn) : null;
      if (normalized != null) {
        return new StringPrimitiveFleakData(normalized);
      }
      return visitStrResult(tn.getText());
    }
    tn = ctx.BOOLEAN_LITERAL();
    if (tn != null) {
      return visitBoolResult(tn.getText());
    }

    tn = ctx.NULL_LITERAL();
    if (tn != null) {
      return null;
    }

    String numStr;
    tn = ctx.NUMBER_LITERAL();
    if (tn != null) {
      numStr = tn.getText();
    } else {
      numStr = ctx.INT_LITERAL().getText();
    }
    if (numStr.contains(".")) {
      return visitNumResult(Double.parseDouble(numStr), NumberPrimitiveFleakData.NumberType.DOUBLE);
    }
    return visitNumResult(Long.parseLong(numStr), NumberPrimitiveFleakData.NumberType.LONG);
  }

  @Override
  public FleakData visitGenericFunctionCall(EvalExpressionParser.GenericFunctionCallContext ctx) {
    String functionName = ctx.IDENTIFIER().getText();
    FeelFunction feelFunction = this.functionsTable.get(functionName);

    if (feelFunction == null) {
      throw new IllegalArgumentException("Unknown function: " + functionName);
    }

    List<EvalExpressionParser.ExpressionContext> args =
        ctx.arguments() != null ? ctx.arguments().expression() : Collections.emptyList();

    // Special handling for Python function which needs context for pre-compilation lookup
    if ("python".equals(functionName) && feelFunction instanceof FeelFunction.PythonFunction) {
      return ((FeelFunction.PythonFunction) feelFunction).evaluateWithContext(this, args, ctx);
    }

    return feelFunction.evaluate(this, args);
  }

  @Override
  public FleakData visitDictExpression(EvalExpressionParser.DictExpressionContext ctx) {
    if (ctx.kvPair() == null || ctx.kvPair().isEmpty()) {
      return new RecordFleakData();
    }

    Map<String, FleakData> payload = new HashMap<>();
    for (EvalExpressionParser.KvPairContext kvPairCtx : ctx.kvPair()) {
      // Use cached dict key if available
      EvalExpressionParser.DictKeyContext dictKeyCtx = kvPairCtx.dictKey();
      String key = cache != null ? cache.getDictKey(dictKeyCtx) : null;
      if (key == null) {
        key = dictKeyCtx.getText();
      }
      FleakData val;
      try {
        val = visit(kvPairCtx.expression());
      } catch (Exception e) {
        if (lenient) {
          val =
              FleakData.wrap(
                  String.format(
                      ">>> Failed to evaluate expression: %s. Reason: %s",
                      kvPairCtx.expression().getText(), e.getMessage()));
        } else {
          throw e;
        }
      }
      payload.put(key, val);
    }
    return new RecordFleakData(payload);
  }

  private FleakData getVariableValue(String name) {
    for (Map<String, FleakData> scope : variableEnvironment) {
      if (scope.containsKey(name)) {
        return scope.get(name);
      }
    }
    throw new RuntimeException("Variable '" + name + "' is not defined");
  }

  @Override
  public FleakData visitPathSelectExpr(EvalExpressionParser.PathSelectExprContext ctx) {
    FleakData rooObject = getVariableValue(ROOT_OBJECT_VARIABLE_NAME);

    if (CollectionUtils.isEmpty(ctx.step())) {
      return rooObject;
    }

    FleakData value = rooObject;
    for (EvalExpressionParser.StepContext stepCtx : ctx.step()) {
      value = applyStep(value, stepCtx);
    }
    return value;
  }

  @Override
  public FleakData visitCaseExpression(EvalExpressionParser.CaseExpressionContext ctx) {
    for (var whenClause : ctx.whenClause()) {
      FleakData conditionResult = visit(whenClause.expression(0));
      if (conditionResult != null && conditionResult.isTrueValue()) {
        return visit(whenClause.expression(1));
      }
    }
    return visit(ctx.elseClause().expression());
  }

  // Canonical integer argument validation method - used by all functions
  public int evalArgAsInt(EvalExpressionParser.ExpressionContext exprCtx, String argName) {
    if (exprCtx == null) {
      throw new IllegalArgumentException("Argument '" + argName + "' is missing");
    }

    FleakData fleakData = visit(exprCtx);
    if (!(fleakData instanceof NumberPrimitiveFleakData)) {
      throw new IllegalArgumentException(
          String.format(
              "Argument '%s' (%s) must be a number, but got: %s",
              argName,
              exprCtx.getText(),
              fleakData != null ? fleakData.getClass().getSimpleName() : "null"));
    }

    double doubleValue = getDoubleValue(exprCtx, argName, fleakData);

    return (int) doubleValue;
  }

  private static double getDoubleValue(
      EvalExpressionParser.ExpressionContext exprCtx, String argName, FleakData fleakData) {
    double doubleValue = fleakData.getNumberValue();

    if (doubleValue > Integer.MAX_VALUE || doubleValue < Integer.MIN_VALUE) {
      throw new IllegalArgumentException(
          String.format(
              "Argument '%s' (%s) value '%s' is out of valid integer range [%d, %d]",
              argName, exprCtx.getText(), doubleValue, Integer.MIN_VALUE, Integer.MAX_VALUE));
    }

    // Check if the number is an integer (e.g., 3.0 is okay, 3.5 is not)
    if (doubleValue % 1 != 0) {
      throw new IllegalArgumentException(
          String.format(
              "Argument '%s' (%s) must be a whole number, but got: %s with a fractional part",
              argName, exprCtx.getText(), doubleValue));
    }
    return doubleValue;
  }

  // Legacy method for backward compatibility
  private int evalArgAsInt(EvalExpressionParser.ExpressionContext exprCtx) {
    return evalArgAsInt(exprCtx, "expression");
  }

  private FleakData visitBinaryNode(ParserRuleContext ctx) {
    FleakData value = visit(ctx.getChild(0));
    if (ctx.getChildCount() == 1) {
      return value;
    }
    for (int i = 1; i < ctx.getChildCount(); i += 2) {
      ParseTree opNode = ctx.getChild(i);
      // Use cached operator text if available
      String op = cache != null ? cache.getOperator(opNode) : opNode.getText();
      FleakData right = visit(ctx.getChild(i + 1));
      EvaluatorFuncs.BinaryEvaluatorFunc<FleakData> evaluator =
          Objects.requireNonNull(BINARY_VALUE_EVALUATOR_FUNC_MAP.get(op));
      value = evaluator.evaluate(value, right);
    }
    return value;
  }

  private FleakData visitStrResult(String text) {
    String normalized = normalizeStrLiteral(text);
    return new StringPrimitiveFleakData(normalized);
  }

  private FleakData visitBoolResult(String text) {
    return new BooleanPrimitiveFleakData(Boolean.parseBoolean(text));
  }

  private FleakData visitNumResult(
      double numberVal, NumberPrimitiveFleakData.NumberType numberType) {
    return new NumberPrimitiveFleakData(numberVal, numberType);
  }

  private FleakData applyStep(FleakData value, EvalExpressionParser.StepContext ctx) {
    if (ctx.fieldAccess() != null) {
      return applyFieldAccess(value, ctx.fieldAccess());
    } else if (ctx.arrayAccess() != null) {
      return applyArrayAccess(value, ctx.arrayAccess());
    } else {
      throw new RuntimeException("Invalid step in variable reference. Step: " + ctx.getText());
    }
  }

  private FleakData applyFieldAccess(FleakData value, EvalExpressionParser.FieldAccessContext ctx) {

    if (!(value instanceof RecordFleakData)) {
      return null;
    }

    // Use cached field name if available
    String fieldName = cache != null ? cache.getFieldName(ctx) : null;
    if (fieldName == null) {
      if (ctx.QUOTED_IDENTIFIER() != null) {
        fieldName = normalizeStrLiteral(ctx.QUOTED_IDENTIFIER().getText());
      } else if (ctx.IDENTIFIER() != null) {
        fieldName = ctx.IDENTIFIER().getText();
      } else {
        throw new RuntimeException("Invalid field access: " + ctx.getText());
      }
    }

    // Retrieve the field from the current value
    return value.getPayload().get(fieldName);
  }

  private FleakData applyArrayAccess(FleakData value, EvalExpressionParser.ArrayAccessContext ctx) {
    if (!(value instanceof ArrayFleakData)) {
      return null;
    }

    int index = evalArgAsInt(ctx.expression());
    if (!validArrayIndex(value.getArrayPayload(), index)) {
      return null;
    }

    return value.getArrayPayload().get(index);
  }

  public void enterScope() {
    variableEnvironment.push(new HashMap<>());
  }

  public void exitScope() {
    variableEnvironment.pop();
  }

  public void setVariable(String name, FleakData value) {
    if ("$".equals(name)) {
      throw new RuntimeException("Cannot redefine root variable '$'");
    }
    Map<String, FleakData> scope = variableEnvironment.peek();
    Preconditions.checkNotNull(scope);
    scope.put(name, value);
  }
}
