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
package io.fleak.zephflow.lib.commands.eval.compiled;

import static io.fleak.zephflow.lib.commands.eval.EvaluatorFuncs.BINARY_VALUE_EVALUATOR_FUNC_MAP;
import static io.fleak.zephflow.lib.commands.eval.EvaluatorFuncs.UNARY_VALUE_EVALUATOR_FUNC_MAP;
import static io.fleak.zephflow.lib.utils.MiscUtils.normalizeStrLiteral;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionBaseVisitor;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.EvaluatorFuncs;
import io.fleak.zephflow.lib.commands.eval.FeelFunction;
import io.fleak.zephflow.lib.commands.eval.compiled.nodes.*;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * Compiles an ANTLR parse tree into a lightweight AST of ExpressionNodes. This compilation happens
 * once, and the resulting AST can be evaluated repeatedly without the overhead of the visitor
 * pattern.
 */
public class ExpressionCompiler extends EvalExpressionBaseVisitor<ExpressionNode> {

  private final Map<String, FeelFunction> functionsTable;
  private final boolean lenient;

  private ExpressionCompiler(PythonExecutor pythonExecutor, boolean lenient) {
    this.functionsTable = FeelFunction.createFunctionsTable(pythonExecutor);
    this.lenient = lenient;
  }

  public static CompiledExpression compile(
      EvalExpressionParser.LanguageContext ctx, PythonExecutor pythonExecutor) {
    return compile(ctx, pythonExecutor, false);
  }

  public static CompiledExpression compile(
      EvalExpressionParser.LanguageContext ctx, PythonExecutor pythonExecutor, boolean lenient) {
    ExpressionCompiler compiler = new ExpressionCompiler(pythonExecutor, lenient);
    ExpressionNode root = compiler.visit(ctx);
    return new CompiledExpression(root);
  }

  @Override
  public ExpressionNode visit(ParseTree tree) {
    if (!lenient) {
      return super.visit(tree);
    }
    try {
      return super.visit(tree);
    } catch (Exception e) {
      return new ErrorNode(e.getMessage());
    }
  }

  public static ExpressionNode compilePathSelectExpr(
      EvalExpressionParser.PathSelectExprContext ctx) {
    ExpressionCompiler compiler = new ExpressionCompiler(null, false);
    return compiler.visitPathSelectExpr(ctx);
  }

  @Override
  public ExpressionNode visitLanguage(EvalExpressionParser.LanguageContext ctx) {
    return visit(ctx.expression());
  }

  @Override
  public ExpressionNode visitExpression(EvalExpressionParser.ExpressionContext ctx) {
    return visit(ctx.conditionalOr());
  }

  @Override
  public ExpressionNode visitConditionalOr(EvalExpressionParser.ConditionalOrContext ctx) {
    return compileLogicalNode(ctx, false);
  }

  @Override
  public ExpressionNode visitConditionalAnd(EvalExpressionParser.ConditionalAndContext ctx) {
    return compileLogicalNode(ctx, true);
  }

  @Override
  public ExpressionNode visitEquality(EvalExpressionParser.EqualityContext ctx) {
    return compileBinaryNode(ctx);
  }

  @Override
  public ExpressionNode visitRelational(EvalExpressionParser.RelationalContext ctx) {
    return compileBinaryNode(ctx);
  }

  @Override
  public ExpressionNode visitAdditive(EvalExpressionParser.AdditiveContext ctx) {
    return compileBinaryNode(ctx);
  }

  @Override
  public ExpressionNode visitMultiplicative(EvalExpressionParser.MultiplicativeContext ctx) {
    return compileBinaryNode(ctx);
  }

  @Override
  public ExpressionNode visitUnary(EvalExpressionParser.UnaryContext ctx) {
    Preconditions.checkArgument(ctx.getChildCount() == 1 || ctx.getChildCount() == 2);
    if (ctx.getChildCount() == 1) {
      return visit(ctx.getChild(0));
    }
    ExpressionNode operand = visit(ctx.getChild(1));
    String op = ctx.getChild(0).getText();
    EvaluatorFuncs.UnaryEvaluatorFunc<FleakData> evaluator =
        Objects.requireNonNull(UNARY_VALUE_EVALUATOR_FUNC_MAP.get(op));
    return new UnaryOpNode(operand, evaluator);
  }

  @Override
  public ExpressionNode visitPrimary(EvalExpressionParser.PrimaryContext ctx) {
    ExpressionNode value;

    if (ctx.getChildCount() >= 3
        && "(".equals(ctx.getChild(0).getText())
        && ")".equals(ctx.getChild(2).getText())) {
      value = visit(ctx.getChild(1));
    } else if (ctx.IDENTIFIER() != null) {
      String varName = ctx.IDENTIFIER().getText();
      value = new VariableNode(varName);
    } else if (ctx.pathSelectExpr() != null) {
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

    List<EvalExpressionParser.StepContext> steps = ctx.step();
    if (steps != null && !steps.isEmpty()) {
      List<StepNode> stepNodes = compileSteps(steps);
      return new PrimaryWithStepsNode(value, stepNodes);
    }

    return value;
  }

  @Override
  public ExpressionNode visitValue(EvalExpressionParser.ValueContext ctx) {
    TerminalNode tn = ctx.QUOTED_IDENTIFIER();
    if (tn != null) {
      String normalized = normalizeStrLiteral(tn.getText());
      return new LiteralNode(new StringPrimitiveFleakData(normalized));
    }

    tn = ctx.BOOLEAN_LITERAL();
    if (tn != null) {
      return new LiteralNode(new BooleanPrimitiveFleakData(Boolean.parseBoolean(tn.getText())));
    }

    tn = ctx.NULL_LITERAL();
    if (tn != null) {
      return new LiteralNode(null);
    }

    String numStr;
    tn = ctx.NUMBER_LITERAL();
    if (tn != null) {
      numStr = tn.getText();
    } else {
      numStr = ctx.INT_LITERAL().getText();
    }

    if (numStr.contains(".")) {
      return new LiteralNode(
          new NumberPrimitiveFleakData(
              Double.parseDouble(numStr), NumberPrimitiveFleakData.NumberType.DOUBLE));
    }
    return new LiteralNode(
        new NumberPrimitiveFleakData(
            Long.parseLong(numStr), NumberPrimitiveFleakData.NumberType.LONG));
  }

  @Override
  public ExpressionNode visitGenericFunctionCall(
      EvalExpressionParser.GenericFunctionCallContext ctx) {
    String functionName = ctx.IDENTIFIER().getText();
    FeelFunction function = functionsTable.get(functionName);

    if (function == null) {
      throw new IllegalArgumentException("Unknown function: " + functionName);
    }

    List<EvalExpressionParser.ExpressionContext> argContexts =
        ctx.arguments() != null ? ctx.arguments().expression() : Collections.emptyList();

    List<ExpressionNode> argNodes = new ArrayList<>();
    List<String> lazyArgTexts = new ArrayList<>();

    for (EvalExpressionParser.ExpressionContext argCtx : argContexts) {
      argNodes.add(visit(argCtx));
      lazyArgTexts.add(argCtx.getText());
    }

    return new FunctionCallNode(functionName, argNodes, function, ctx, lazyArgTexts);
  }

  @Override
  public ExpressionNode visitDictExpression(EvalExpressionParser.DictExpressionContext ctx) {
    List<EvalExpressionParser.KvPairContext> kvPairs = ctx.kvPair();
    if (kvPairs == null || kvPairs.isEmpty()) {
      return new DictNode(Collections.emptyList());
    }

    List<DictNode.KvPair> compiledPairs = new ArrayList<>();
    for (EvalExpressionParser.KvPairContext kvPairCtx : kvPairs) {
      String key = kvPairCtx.dictKey().getText();
      if (kvPairCtx.dictKey().QUOTED_IDENTIFIER() != null) {
        key = normalizeStrLiteral(key);
      }
      ExpressionNode valueNode = visit(kvPairCtx.expression());
      String originalExprText = kvPairCtx.expression().getText();
      compiledPairs.add(new DictNode.KvPair(key, valueNode, originalExprText));
    }

    return new DictNode(compiledPairs);
  }

  @Override
  public ExpressionNode visitPathSelectExpr(EvalExpressionParser.PathSelectExprContext ctx) {
    List<EvalExpressionParser.StepContext> steps = ctx.step();
    if (steps == null || steps.isEmpty()) {
      return new VariableNode("$");
    }

    List<StepNode> stepNodes = compileSteps(steps);
    return new PathSelectNode(stepNodes);
  }

  @Override
  public ExpressionNode visitCaseExpression(EvalExpressionParser.CaseExpressionContext ctx) {
    List<CaseNode.WhenClause> whenClauses = new ArrayList<>();
    for (var whenClause : ctx.whenClause()) {
      ExpressionNode condition = visit(whenClause.expression(0));
      ExpressionNode result = visit(whenClause.expression(1));
      whenClauses.add(new CaseNode.WhenClause(condition, result));
    }
    ExpressionNode elseResult = visit(ctx.elseClause().expression());
    return new CaseNode(whenClauses, elseResult);
  }

  private List<StepNode> compileSteps(List<EvalExpressionParser.StepContext> steps) {
    List<StepNode> stepNodes = new ArrayList<>();
    for (EvalExpressionParser.StepContext stepCtx : steps) {
      stepNodes.add(compileStep(stepCtx));
    }
    return stepNodes;
  }

  private StepNode compileStep(EvalExpressionParser.StepContext ctx) {
    if (ctx.fieldAccess() != null) {
      return compileFieldAccess(ctx.fieldAccess());
    } else if (ctx.arrayAccess() != null) {
      return compileArrayAccess(ctx.arrayAccess());
    } else {
      throw new RuntimeException("Invalid step in variable reference. Step: " + ctx.getText());
    }
  }

  private StepNode compileFieldAccess(EvalExpressionParser.FieldAccessContext ctx) {
    String fieldName;
    if (ctx.QUOTED_IDENTIFIER() != null) {
      fieldName = normalizeStrLiteral(ctx.QUOTED_IDENTIFIER().getText());
    } else if (ctx.IDENTIFIER() != null) {
      fieldName = ctx.IDENTIFIER().getText();
    } else {
      throw new RuntimeException("Invalid field access: " + ctx.getText());
    }
    return new FieldAccessStepNode(fieldName);
  }

  private StepNode compileArrayAccess(EvalExpressionParser.ArrayAccessContext ctx) {
    ExpressionNode indexNode = visit(ctx.expression());
    return new ArrayAccessStepNode(indexNode);
  }

  private ExpressionNode compileBinaryNode(ParserRuleContext ctx) {
    ExpressionNode result = visit(ctx.getChild(0));
    if (ctx.getChildCount() == 1) {
      return result;
    }
    for (int i = 1; i < ctx.getChildCount(); i += 2) {
      ParseTree opNode = ctx.getChild(i);
      String op = opNode.getText();
      ExpressionNode right = visit(ctx.getChild(i + 1));
      EvaluatorFuncs.BinaryEvaluatorFunc<FleakData> evaluator =
          Objects.requireNonNull(BINARY_VALUE_EVALUATOR_FUNC_MAP.get(op));
      result = new BinaryOpNode(result, right, evaluator);
    }
    return result;
  }

  private ExpressionNode compileLogicalNode(ParserRuleContext ctx, boolean isAnd) {
    ExpressionNode result = visit(ctx.getChild(0));
    if (ctx.getChildCount() == 1) {
      return result;
    }
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      ExpressionNode right = visit(ctx.getChild(i));
      if (isAnd) {
        result = new LogicalAndNode(result, right);
      } else {
        result = new LogicalOrNode(result, right);
      }
    }
    return result;
  }
}
