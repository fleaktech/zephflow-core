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
package io.fleak.zephflow.lib.sql.exec;

import io.fleak.zephflow.lib.sql.ast.QueryAST;
import io.fleak.zephflow.lib.sql.ast.types.LikeStringPattern;
import io.fleak.zephflow.lib.sql.errors.ErrorBeautifier;
import io.fleak.zephflow.lib.sql.errors.SQLRuntimeError;
import io.fleak.zephflow.lib.sql.exec.types.BooleanLogic;
import io.fleak.zephflow.lib.sql.exec.types.SQLTypeSystem;
import java.util.ArrayList;
import org.apache.commons.lang3.BooleanUtils;

/** Interprets QueryAST Expressions. */
public class QueryASTInterpreter extends Interpreter<QueryAST.Datum> {

  private final SQLTypeSystem typeSystem;

  public QueryASTInterpreter(SQLTypeSystem typeSystem) {
    this.typeSystem = typeSystem;
  }

  @Override
  public <R> R eval(Lookup context, QueryAST.Datum input, Class<R> clz) {
    try {
      return interpret(context, input, clz);
    } catch (Throwable t) {
      throw new SQLRuntimeError(ErrorBeautifier.beautifyRuntimeErrorMessage(t.getMessage()), t);
    }
  }

  private <R> R interpretExpression(Lookup context, QueryAST.Expr input, Class<R> clz) {
    if (input instanceof QueryAST.BinaryArithmeticExpr expr) {
      var left = interpret(context, expr.getLeft(), Object.class);
      if (left == null) return null;

      var leftArithmetic = typeSystem.lookupTypeArithmetic(left.getClass());
      var right = interpret(context, expr.getRight(), left.getClass());

      if (right == null) return null;

      var typeCast = typeSystem.lookupTypeCast(clz);
      return switch (expr.getOp()) {
        case ADD -> typeCast.cast(leftArithmetic.add(left, right));
        case SUBTRACT -> typeCast.cast(leftArithmetic.sub(left, right));
        case MULTIPLY -> typeCast.cast(leftArithmetic.mul(left, right));
        case DIVIDE -> typeCast.cast(leftArithmetic.div(left, right));
        case MODULUS -> typeCast.cast(leftArithmetic.mod(left, right));
        case POWER -> typeCast.cast(leftArithmetic.power(left, right));
        case STRING_CONCAT ->
            throw new RuntimeException(
                "string literal concat must be transformed to a concat function call");
      };

    } else if (input instanceof QueryAST.BinaryBooleanExpr expr) {
      var left = interpret(context, expr.getLeft(), Object.class);
      if (left == null) return null;

      switch (expr.getOp()) {
        case GREATER_THAN:
          return clz.cast(
              typeSystem
                  .lookupTypeBoolean(left.getClass())
                  .greaterThan(left, interpret(context, expr.getRight(), Object.class)));
        case LESS_THAN:
          return clz.cast(
              typeSystem
                  .lookupTypeBoolean(left.getClass())
                  .lessThan(left, interpret(context, expr.getRight(), Object.class)));
        case LESS_THAN_OR_EQUAL:
          return clz.cast(
              typeSystem
                  .lookupTypeBoolean(left.getClass())
                  .lessThanOrEqual(left, interpret(context, expr.getRight(), Object.class)));
        case GREATER_THAN_OR_EQUAL:
          return clz.cast(
              typeSystem
                  .lookupTypeBoolean(left.getClass())
                  .greaterThanOrEqual(left, interpret(context, expr.getRight(), Object.class)));
        case EQUAL:
          return clz.cast(
              typeSystem
                  .lookupTypeBoolean(left.getClass())
                  .equal(left, interpret(context, expr.getRight(), Object.class)));
        case NOT_EQUAL:
          return clz.cast(
              typeSystem
                  .lookupTypeBoolean(left.getClass())
                  .notEqual(left, interpret(context, expr.getRight(), Object.class)));
        case AND:
          if ((Boolean) left) {
            return clz.cast(interpret(context, expr.getRight(), Boolean.class));
          }
          return clz.cast(Boolean.FALSE);
        case OR:
          if ((Boolean) left) {
            return clz.cast(Boolean.TRUE);
          } else {
            return clz.cast(interpret(context, expr.getRight(), Boolean.class));
          }
        case LIKE:
        case ILIKE:
          return clz.cast(
              runMatcher(
                  (String) left, interpret(context, expr.getRight(), Object.class), expr.getOp()));
        case NOT_LIKE:
        case NOT_ILIKE:
          return clz.cast(
              !runMatcher(
                  (String) left, interpret(context, expr.getRight(), Object.class), expr.getOp()));
        case IN:
          return clz.cast(
              typeSystem
                  .lookupTypeBoolean(left.getClass())
                  .in(left, interpret(context, expr.getRight(), Object.class)));
        case NOT_IN:
          return clz.cast(
              typeSystem
                  .lookupTypeBoolean(left.getClass())
                  .notIn(left, interpret(context, expr.getRight(), Object.class)));
        default:
          throw new RuntimeException(expr.getOp() + " is not supported here");
      }
    } else if (input instanceof QueryAST.Constant) {
      return clz.cast(((QueryAST.Constant) input).getValue());
    } else if (input instanceof QueryAST.UnaryBooleanExpr expr) {
      var left = interpret(context, expr.getExpr(), Object.class);

      var op = expr.getOp();
      return switch (op) {
        case NOT -> clz.cast(left == null || !(Boolean) left);
        case IS_NULL -> clz.cast(left == null);
        case IS_NOT_NULL -> clz.cast(left != null);
      };

    } else if (input instanceof QueryAST.FuncCall expr) {

      var fn = typeSystem.lookupFunction(expr.getFunctionName());
      var exprArgs = expr.getArgs();
      var fnResolvedArgs = new ArrayList<>(exprArgs.size());

      for (var arg : exprArgs) {
        fnResolvedArgs.add(interpret(context, arg, Object.class));
      }

      return clz.cast(fn.apply(fnResolvedArgs));
    } else if (input instanceof QueryAST.CaseWhenExpr caseWhenExpr) {

      for (var whenExpr : caseWhenExpr.getWhenExprs()) {
        // important. Use BooleanUtils here. It automatically guards against null. Otherwise we risk
        // getting a NPE exception
        // when the JVM evals the if condition.
        if (BooleanUtils.isTrue(interpret(context, whenExpr.getWhen(), Boolean.class))) {
          return interpret(context, whenExpr.getResult(), clz);
        }
      }

      if (caseWhenExpr.getElseExpr() == null || caseWhenExpr.getElseExpr().getResult() == null)
        return null;

      return interpret(context, caseWhenExpr.getElseExpr().getResult(), clz);
    }

    throw new RuntimeException(input + " type not expected here");
  }

  private boolean runMatcher(String left, Object right, QueryAST.BinaryBooleanOperator op) {
    BooleanLogic<Object> type;

    if (right instanceof String s && LikeStringPattern.isMatchString(s)) {
      type = typeSystem.lookupTypeBoolean(LikeStringPattern.class);
      right = new LikeStringPattern(s, op == QueryAST.BinaryBooleanOperator.LIKE);
    } else {
      type = typeSystem.lookupTypeBoolean(String.class);
    }

    if (op == QueryAST.BinaryBooleanOperator.ILIKE) {
      return type.ilike(left, right);
    } else if (op == QueryAST.BinaryBooleanOperator.NOT_ILIKE) {
      return type.notILike(left, right);
    } else if (op == QueryAST.BinaryBooleanOperator.LIKE) {
      return type.like(left, right);
    } else if (op == QueryAST.BinaryBooleanOperator.NOT_LIKE) {
      return type.notLike(left, right);
    } else {
      throw new RuntimeException(op + " is not supported here");
    }
  }

  private <R> R interpret(Lookup context, QueryAST.Datum input, Class<R> clz) {

    if (input instanceof QueryAST.Constant) {
      return typeSystem.lookupTypeCast(clz).cast(((QueryAST.Constant) input).getValue());
    } else if (input instanceof QueryAST.SimpleColumn col) {
      return context.resolve(col, clz);
    } else if (input instanceof QueryAST.Null) {
      return null;
    } else if (input instanceof QueryAST.AllColumn) {
      throw new RuntimeException("* not expected here");
    } else if (input instanceof QueryAST.Expr) {
      return interpretExpression(context, (QueryAST.Expr) input, clz);
    } else {
      throw new RuntimeException(input + " is not expected here");
    }
  }
}
