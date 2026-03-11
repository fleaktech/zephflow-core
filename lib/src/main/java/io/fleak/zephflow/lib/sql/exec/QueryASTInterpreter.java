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
    return switch (input) {
      case QueryAST.BinaryArithmeticExpr expr -> {
        var left = interpret(context, expr.getLeft(), Object.class);
        if (left == null) yield null;

        var leftArithmetic = typeSystem.lookupTypeArithmetic(left.getClass());
        var right = interpret(context, expr.getRight(), left.getClass());

        if (right == null) yield null;

        var typeCast = typeSystem.lookupTypeCast(clz);
        yield switch (expr.getOp()) {
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
      }
      case QueryAST.BinaryBooleanExpr expr -> {
        var left = interpret(context, expr.getLeft(), Object.class);
        if (left == null) yield null;

        var boolLogic = typeSystem.lookupTypeBoolean(left.getClass());
        var right = interpret(context, expr.getRight(), Object.class);
        yield clz.cast(
            switch (expr.getOp()) {
              case GREATER_THAN -> boolLogic.greaterThan(left, right);
              case LESS_THAN -> boolLogic.lessThan(left, right);
              case LESS_THAN_OR_EQUAL -> boolLogic.lessThanOrEqual(left, right);
              case GREATER_THAN_OR_EQUAL -> boolLogic.greaterThanOrEqual(left, right);
              case EQUAL -> boolLogic.equal(left, right);
              case NOT_EQUAL -> boolLogic.notEqual(left, right);
              case AND ->
                  (Boolean) left
                      ? interpret(context, expr.getRight(), Boolean.class)
                      : Boolean.FALSE;
              case OR ->
                  (Boolean) left
                      ? Boolean.TRUE
                      : interpret(context, expr.getRight(), Boolean.class);
              case LIKE, ILIKE -> runMatcher((String) left, right, expr.getOp());
              case NOT_LIKE, NOT_ILIKE -> !runMatcher((String) left, right, expr.getOp());
              case IN -> boolLogic.in(left, right);
              case NOT_IN -> boolLogic.notIn(left, right);
              default -> throw new RuntimeException(expr.getOp() + " is not supported here");
            });
      }
      case QueryAST.Constant constant -> clz.cast(constant.getValue());
      case QueryAST.UnaryBooleanExpr expr -> {
        var left = interpret(context, expr.getExpr(), Object.class);
        yield switch (expr.getOp()) {
          case NOT -> clz.cast(left == null || !(Boolean) left);
          case IS_NULL -> clz.cast(left == null);
          case IS_NOT_NULL -> clz.cast(left != null);
        };
      }
      case QueryAST.FuncCall expr -> {
        var fn = typeSystem.lookupFunction(expr.getFunctionName());
        var exprArgs = expr.getArgs();
        var fnResolvedArgs = new ArrayList<>(exprArgs.size());
        for (var arg : exprArgs) {
          fnResolvedArgs.add(interpret(context, arg, Object.class));
        }
        yield clz.cast(fn.apply(fnResolvedArgs));
      }
      case QueryAST.CaseWhenExpr caseWhenExpr -> {
        R result = null;
        for (var whenExpr : caseWhenExpr.getWhenExprs()) {
          if (BooleanUtils.isTrue(interpret(context, whenExpr.getWhen(), Boolean.class))) {
            result = interpret(context, whenExpr.getResult(), clz);
            yield result;
          }
        }
        if (caseWhenExpr.getElseExpr() == null || caseWhenExpr.getElseExpr().getResult() == null)
          yield null;
        yield interpret(context, caseWhenExpr.getElseExpr().getResult(), clz);
      }
      default -> throw new RuntimeException(input + " type not expected here");
    };
  }

  private boolean runMatcher(String left, Object right, QueryAST.BinaryBooleanOperator op) {
    BooleanLogic<Object> type;

    if (right instanceof String s && LikeStringPattern.isMatchString(s)) {
      type = typeSystem.lookupTypeBoolean(LikeStringPattern.class);
      right = new LikeStringPattern(s, op == QueryAST.BinaryBooleanOperator.LIKE);
    } else {
      type = typeSystem.lookupTypeBoolean(String.class);
    }

    return switch (op) {
      case ILIKE -> type.ilike(left, right);
      case NOT_ILIKE -> type.notILike(left, right);
      case LIKE -> type.like(left, right);
      case NOT_LIKE -> type.notLike(left, right);
      default -> throw new RuntimeException(op + " is not supported here");
    };
  }

  private <R> R interpret(Lookup context, QueryAST.Datum input, Class<R> clz) {
    return switch (input) {
      case QueryAST.Constant constant -> typeSystem.lookupTypeCast(clz).cast(constant.getValue());
      case QueryAST.SimpleColumn col -> context.resolve(col, clz);
      case QueryAST.Null n -> null;
      case QueryAST.AllColumn a -> throw new RuntimeException("* not expected here");
      case QueryAST.Expr expr -> interpretExpression(context, expr, clz);
      default -> throw new RuntimeException(input + " is not expected here");
    };
  }
}
