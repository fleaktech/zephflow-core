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
package io.fleak.zephflow.lib.sql.ast;

import static io.fleak.zephflow.lib.utils.AntlrUtils.cleanIdentifier;
import static io.fleak.zephflow.lib.utils.AntlrUtils.cleanStringLiteral;

import com.google.common.base.Strings;
import io.fleak.zephflow.lib.antlr.ExpressionSQLParserBaseListener;
import io.fleak.zephflow.lib.antlr.ExpressionSQLParserParser;
import io.fleak.zephflow.lib.sql.exec.functions.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Techniques used:
 *
 * <p>Mark and Gather.<br>
 *
 * <p>Expressions like <code>select col1, col2, col3 from</code> we define the grammar to be <code>
 * select selectList from</code> When we enter the selectList we mark the place in the stack e.g.
 * with a SELECT_LIST_START. Then the listener can process any amount of recursive data, but when we
 * hit the exit selectList event, we expect all data up to the SELECT_LIST_START to be valid columns
 * or expressions. At this point we pop the stack and until we get a SELECT_LIST_START, then add all
 * the popped items to the select part of the query. This last part is the Gather phase.
 *
 * <p>Logic:
 *
 * <p>
 *
 * <pre>
 * Our definition of a select statement is: $q = select $S from $F where $W
 *
 *  $S can contain expressions $E,like 1+2 or identifiers $i like a or a.b all identifiers are separated.
 *  Each expression in $S is separated with a comma with an optional alias. So $S = $s1, $s2, $s3
 *
 *  $F can contain identifiers or select statements $Q.
 *  Each identifier in $F, $f or in $Q,  $q are separated with a comma and has an optional alias for $f and a required alias for $q.
 *
 *  $W can contain one boolean expression with nested boolean expressions $b.
 *  $b can be a = 2 or a > 10.
 *
 *
 *
 * </pre>
 */
public class ExpressionToASTListener extends ExpressionSQLParserBaseListener {

  private final AtomicInteger anonymousRelNameCounter = new AtomicInteger();
  private final AtomicInteger anonymousColumnNameCounter = new AtomicInteger();

  private final Set<String> setReturningFunctionNames =
      Set.of(JsonArrayElements.NAME, RegexpSplitToTable.NAME);
  private final Set<String> aggregateFunctionNames =
      Set.of(
          CountAgg.NAME,
          SumAgg.NAME,
          MinAgg.NAME,
          MaxAgg.NAME,
          "avg",
          StringAgg.NAME,
          JsonAgg.NAME);

  Deque<Object> stack = new ArrayDeque<>();
  Deque<QueryAST.Query> queryStack = new ArrayDeque<>();

  List<QueryAST.From> setReturningFunctions = new ArrayList<>();
  List<QueryAST.FuncCall> aggregateFunctions = new ArrayList<>();

  /**
   * Helps when we create columns to know if we are creating a from identifier or a select
   * identifier.
   */
  Deque<Placeholders> modeStack = new ArrayDeque<>();

  QueryAST.Query innerQuery;

  /** */
  enum Placeholders {
    SELECT_LIST_START,
    DISTINCT_ON,
    FUNCTION_START,
    FROM_START,
    WHERE_START,
    GROUP_BY_START,
    HAVING_START,
    ORDER_BY_START,
    SET_RETURNING_FUNCTION_START,
    AGGREGATE_FUNCTION_START,
    JOIN_START,
    CASE_WHEN_START,
    CASE_WHEN_CONDITION_START
  }

  @SuppressWarnings("method")
  public QueryAST.Query getInnerQuery() {
    return innerQuery;
  }

  @Override
  public void exitSelectStatement(ExpressionSQLParserParser.SelectStatementContext ctx) {
    // on the top most query this will produce an empty stack and innerQuery will be the root query
    // if the stack is not empty then the stackHead is the parent query and the innerQuery is the
    // direct inner query
    // last processed
    innerQuery = queryStack.pop();
    var mode = modeStack.peek();
    if (innerQuery != null) {
      if (mode == Placeholders.FROM_START) {
        stack.push(QueryAST.fromClauseQuery(innerQuery));
      } else {
        stack.push(innerQuery);
      }
    }
  }

  @Override
  public void enterSelectStatement(ExpressionSQLParserParser.SelectStatementContext ctx) {
    queryStack.push(new QueryAST.Query());
    setReturningFunctions = new ArrayList<>();
    aggregateFunctions = new ArrayList<>();
  }

  public void enterSelectList(ExpressionSQLParserParser.SelectListContext ctx) {
    modeStack.push(Placeholders.SELECT_LIST_START);
    stack.push(Placeholders.SELECT_LIST_START);
  }

  @Override
  public void exitSelectList(ExpressionSQLParserParser.SelectListContext ctx) {
    modeStack.pop();
    List<QueryAST.Datum> args =
        popToMarkerAndReverse(Placeholders.SELECT_LIST_START, assertIsType(QueryAST.Datum.class));
    var q = queryStack.peek();
    if (q == null) throw new RuntimeException("query cannot be null after exiting a select clause");
    q.setColumns(args);
    q.updateLateralFroms(setReturningFunctions);
  }

  @Override
  public void enterWhereClause(ExpressionSQLParserParser.WhereClauseContext ctx) {
    modeStack.push(Placeholders.WHERE_START);
    stack.push(Placeholders.WHERE_START);
  }

  @Override
  public void exitWhereClause(ExpressionSQLParserParser.WhereClauseContext ctx) {
    modeStack.pop();
    var datums =
        popToMarkerAndReverse(Placeholders.WHERE_START, assertIsType(QueryAST.BooleanExpr.class));
    if (!datums.isEmpty()) {
      if (datums.size() > 1) {
        throw new RuntimeException(
            "we expect a boolean expression in the where clause but got "
                + datums.size()
                + " different expressions"
                + datums.stream().map(Object::toString).collect(Collectors.joining(",")));
      }

      var q = queryStack.peek();
      if (q == null)
        throw new RuntimeException("query cannot be null after exiting a where clause");

      q.setWhere(datums.get(0));
    }
  }

  @Override
  public void enterGroupByClause(ExpressionSQLParserParser.GroupByClauseContext ctx) {
    modeStack.push(Placeholders.GROUP_BY_START);
    stack.push(Placeholders.GROUP_BY_START);
  }

  @Override
  public void exitGroupByClause(ExpressionSQLParserParser.GroupByClauseContext ctx) {
    modeStack.pop();
    var datums =
        popToMarkerAndReverse(Placeholders.GROUP_BY_START, assertIsType(QueryAST.Datum.class));
    if (!datums.isEmpty()) {
      var q = queryStack.peek();
      if (q == null)
        throw new RuntimeException("query cannot be null after exiting a group by clause");

      q.setGroupBy(datums);
    }
  }

  @Override
  public void enterFromClause(ExpressionSQLParserParser.FromClauseContext ctx) {
    modeStack.push(Placeholders.FROM_START);
    stack.push(Placeholders.FROM_START);
  }

  @Override
  public void exitFromClause(ExpressionSQLParserParser.FromClauseContext ctx) {
    var args = popToMarkerAndReverse(Placeholders.FROM_START, assertIsType(QueryAST.From.class));
    modeStack.pop();

    var q = queryStack.peek();
    if (q == null) throw new RuntimeException("query cannot be null after exiting a from clause");

    var froms = new ArrayList<QueryAST.From>();
    var laterals = new ArrayList<QueryAST.From>();

    for (QueryAST.From a : args) {
      if (a instanceof QueryAST.LateralFuncFrom lf) {
        // check if the schema is null
        if (lf.getSchema().isEmpty()) {
          // create a new schema with a single column where the column name is equal to the alias of
          // the function alias
          lf.setSchema(
              List.of(QueryAST.columnFromString(lf.getAliasOrName(), lf.getAliasOrName())));
        }
        laterals.add(lf);
      } else {
        froms.add(a);
      }
    }
    q.setFrom(froms);
    q.updateLateralFroms(laterals);
    q.updateAggregateFunctions(aggregateFunctions);
  }

  @Override
  public void enterJoinClause(ExpressionSQLParserParser.JoinClauseContext ctx) {
    throw new RuntimeException("join clauses are not yet supported");
    //    modeStack.push(Placeholders.JOIN_START);
    //    stack.push(Placeholders.JOIN_START);
  }

  @Override
  public void exitJoinClause(ExpressionSQLParserParser.JoinClauseContext ctx) {
    modeStack.pop();
    var args =
        popToMarkerAndReverse(Placeholders.JOIN_START, assertIsType(QueryAST.JoinFrom.class));

    var q = queryStack.peek();
    if (q == null) throw new RuntimeException("query cannot be null after exiting a join clause");

    q.addJoins(args);
  }

  @Override
  public void enterJoinItem(ExpressionSQLParserParser.JoinItemContext ctx) {
    QueryAST.CoreJoinOp coreJoinOp = null;
    QueryAST.DirectionalJoinOp directionalJoinOp = null;

    if (ctx.directionOp != null) {
      coreJoinOp = QueryAST.CoreJoinOp.valueOf(ctx.directionOp.getText().toUpperCase());
    }
    if (ctx.directionOp != null) {
      directionalJoinOp =
          QueryAST.DirectionalJoinOp.valueOf(ctx.directionOp.getText().toUpperCase());
    }
    stack.push(new Object[] {coreJoinOp, directionalJoinOp});
  }

  @Override
  public void exitJoinItem(ExpressionSQLParserParser.JoinItemContext ctx) {
    // we expect a stack with [ [CoreJoinOp nullable, DirJoinOp nullable], Column, BooleanExpr ]
    // @TODO when adding support for subselect in join change the Column to be either a subselect or
    // column
    assertStackItems(3);

    var joinExpr = assertIsType(QueryAST.BooleanExpr.class).apply(stack.pop());
    var joinFrom = assertIsType(QueryAST.From.class).apply(stack.pop());
    var ops = assertIsType(Object[].class).apply(stack.pop());
    if (ops.length != 2) {
      throw new RuntimeException(
          "internal error, the join ops must have length 2 here; " + ops.length);
    }

    var joinFromClause =
        QueryAST.fromJoin(
            (QueryAST.DirectionalJoinOp) ops[0], (QueryAST.CoreJoinOp) ops[1], joinFrom, joinExpr);

    stack.push(joinFromClause);
  }

  @Override
  public void enterHavingClause(ExpressionSQLParserParser.HavingClauseContext ctx) {
    throw new RuntimeException("having clauses is not supported yet");
    //    modeStack.push(Placeholders.HAVING_START);
    //    stack.push(Placeholders.HAVING_START);
  }

  @Override
  public void exitHavingClause(ExpressionSQLParserParser.HavingClauseContext ctx) {
    modeStack.pop();
    var datums =
        popToMarkerAndReverse(Placeholders.HAVING_START, assertIsType(QueryAST.BooleanExpr.class));

    if (!datums.isEmpty()) {
      if (datums.size() > 1) {
        throw new RuntimeException(
            "we expect a boolean expression in the having clause but got "
                + datums.size()
                + " different expressions");
      }

      var q = queryStack.peek();
      if (q == null)
        throw new RuntimeException("query cannot be null after exiting a having clause");

      q.setHaving(datums.get(0));
    }
  }

  @Override
  public void enterOrderByClause(ExpressionSQLParserParser.OrderByClauseContext ctx) {
    modeStack.push(Placeholders.ORDER_BY_START);
    stack.push(Placeholders.ORDER_BY_START);
  }

  @Override
  public void exitOrderByClause(ExpressionSQLParserParser.OrderByClauseContext ctx) {
    modeStack.pop();
    var datums =
        popToMarkerAndReverse(Placeholders.ORDER_BY_START, assertIsType(QueryAST.Datum.class));
    int datumIndex = 0;
    if (!datums.isEmpty()) {
      boolean[] asc = new boolean[datums.size()];
      Arrays.fill(asc, true);

      for (var child : ctx.children) {
        var txt = child.getText();
        if (txt.equals(",")) {
          datumIndex++;
        }
        if (txt.equalsIgnoreCase("desc")) {
          asc[datumIndex] = false;
        }
      }

      for (int i = 0; i < datums.size(); i++) {
        datums.get(i).setAsc(asc[i]);
      }
    }

    var q = queryStack.peek();
    if (q == null)
      throw new RuntimeException("query cannot be null after exiting a order by clause");

    q.setOrderBy(datums);
  }

  @Override
  public void exitOtherBinaryBooleanExpr(
      ExpressionSQLParserParser.OtherBinaryBooleanExprContext ctx) {
    assertStackItems(2);
    pushBinaryBooleanExpr(ctx.OTHER_BINARY_BOOLEAN().getText());
  }

  @Override
  public void exitOtherBinaryBooleanExpr2(
      ExpressionSQLParserParser.OtherBinaryBooleanExpr2Context ctx) {
    assertStackItems(2);
    pushBinaryBooleanExpr(ctx.OTHER_BINARY_BOOLEAN().getText());
  }

  @Override
  public void exitArithmeticBinaryBooleanExpr2(
      ExpressionSQLParserParser.ArithmeticBinaryBooleanExpr2Context ctx) {
    assertStackItems(2);
    pushBinaryBooleanExpr(ctx.ARITHMETIC_BINARY_BOOLEAN().getText());
  }

  @Override
  public void exitAritheticBooleanExpr(ExpressionSQLParserParser.AritheticBooleanExprContext ctx) {
    assertStackItems(2);
    pushBinaryBooleanExpr(ctx.ARITHMETIC_BINARY_BOOLEAN().getText());
  }

  @Override
  public void exitLogicalBinaryBooleanExpr2(
      ExpressionSQLParserParser.LogicalBinaryBooleanExpr2Context ctx) {
    assertStackItems(2);
    pushBinaryBooleanExpr(ctx.LOGIC_BINARY_BOOLEAN().getText());
  }

  @Override
  public void exitLogicalBooleanExpr(ExpressionSQLParserParser.LogicalBooleanExprContext ctx) {
    assertStackItems(2);
    pushBinaryBooleanExpr(ctx.LOGIC_BINARY_BOOLEAN().getText());
  }

  @Override
  public void exitMultiplicateBinaryExpr(
      ExpressionSQLParserParser.MultiplicateBinaryExprContext ctx) {
    assertStackItems(2);
    pushBinaryArithmeticExpr(ctx.BINARY_MULTIPLICATIVE().getText());
  }

  @Override
  public void exitAdditiveBinaryExpr(ExpressionSQLParserParser.AdditiveBinaryExprContext ctx) {
    assertStackItems(2);

    var operatorString = ctx.BINARY_ADDITIVE().getText();
    if (operatorString.equals("||")) {
      pushStringLiteralConcatToConcatFunctionTransform();
    } else {
      pushBinaryArithmeticExpr(operatorString);
    }
  }

  @Override
  public void exitRightMultiplicativeBinaryExpr(
      ExpressionSQLParserParser.RightMultiplicativeBinaryExprContext ctx) {
    assertStackItems(1);
    pushBinaryArithmeticExpr(ctx.BINARY_RIGHT_MULTIPLCATIVE().getText());
  }

  @Override
  public void exitSuffixUnaryBooleanExpr2(
      ExpressionSQLParserParser.SuffixUnaryBooleanExpr2Context ctx) {
    assertStackItems(1);
    pushUnaryBooleanExpr(ctx.SUFFIX_UNARY_BOOLEAN().getText());
  }

  @Override
  public void exitExprOptionalRename(ExpressionSQLParserParser.ExprOptionalRenameContext ctx) {
    // after exiting a optionalRename clause, check the newName and if present rename the last item
    // on the stack.
    if (ctx.newName != null) {
      assertStackItems(1);

      Object stackItem = stack.peek();
      assertIsType(QueryAST.Datum.class)
          .apply(stackItem)
          .setAlias(cleanIdentifier(ctx.newName.getText()));
    }
  }

  @Override
  public void exitSelectAllColumns(ExpressionSQLParserParser.SelectAllColumnsContext ctx) {
    stack.push(QueryAST.allColumn(null));
  }

  @Override
  public void exitDistinctExpr(ExpressionSQLParserParser.DistinctExprContext ctx) {
    assertIsType(QueryAST.Datum.class).apply(stack.peek()).setDistinct(true);
  }

  @Override
  public void enterDistinctOn(ExpressionSQLParserParser.DistinctOnContext ctx) {
    stack.push(Placeholders.DISTINCT_ON);
  }

  @Override
  public void exitDistinctOn(ExpressionSQLParserParser.DistinctOnContext ctx) {
    List<QueryAST.Datum> args =
        popToMarkerAndReverse(Placeholders.DISTINCT_ON, assertIsType(QueryAST.Datum.class));

    var q = queryStack.peek();
    if (q == null)
      throw new RuntimeException("query cannot be null after exiting a distinct on clause");

    q.setDistinctOn(args);
  }

  @Override
  public void enterFuncCallExpr(ExpressionSQLParserParser.FuncCallExprContext ctx) {
    stack.push(Placeholders.FUNCTION_START);
    var functionName = ctx.FUNCTION_NAMES().getText().toLowerCase();
    if (setReturningFunctionNames.contains(functionName)) {
      modeStack.push(Placeholders.SET_RETURNING_FUNCTION_START);
    } else if (aggregateFunctionNames.contains(functionName)) {
      modeStack.push(Placeholders.AGGREGATE_FUNCTION_START);
    }
  }

  @Override
  public void exitFuncCallExpr(ExpressionSQLParserParser.FuncCallExprContext ctx) {
    // pop the stack till we reach the FUNCTION_START placeholder
    List<QueryAST.Datum> args =
        popToMarkerAndReverse(Placeholders.FUNCTION_START, assertIsType(QueryAST.Datum.class));

    // for the case of count(*)
    if (ctx.BINARY_MULTIPLICATIVE() != null
        && !Strings.isNullOrEmpty(ctx.BINARY_MULTIPLICATIVE().getText())) {
      args.clear();
    }

    var functionName = ctx.FUNCTION_NAMES().getText().toLowerCase();
    var function = QueryAST.function(functionName, args);

    if (ctx.DISTINCT() != null) {
      function.setDistinct(true);
    }

    if (aggregateFunctionNames.contains(functionName)) {
      modeStack.pop();
      if (modeStack.peek() == Placeholders.SELECT_LIST_START) {

        // replace the function expression in the select clause with a generated column
        // and lift the function into the aggregate functions list.
        var columnName = anonymousColumnName();
        var column = QueryAST.columnFromString(null, columnName);

        // we tie the function expression name to that of the column we are inserting into
        function.setName(columnName);

        aggregateFunctions.add(function);
        stack.push(column);

      } else if (modeStack.peek() == Placeholders.HAVING_START) {
        throw new RuntimeException(
            "aggregate functions in the having clause are not implemented yet");
      } else {
        throw new RuntimeException(functionName + " aggregate function is not allowed here");
      }
    } else if (setReturningFunctionNames.contains(functionName)) {
      modeStack.pop();

      /* ------------------- Set returning functions in select clauses as lateral joins -------------------------
       * Represents a function that returns a set for each call and that is part of a lateral join.
       * When a set returning function is present in the select clause we do:
       *   For example: select fn(args)->'a' from tbl where fn is a set returning function
       *   We move it to the lateral from clauses, give it a relational name, and a single column schema
       *   Like so: select rel_1.col_1->'1' from tbl lateral join fn(args) as rel_1
       *   Args can be any expression and can reference any field from tbl.
       */
      if (modeStack.peek() == Placeholders.SELECT_LIST_START) {
        // Lift the function call into the lateral joins, give it a unique relation name and a
        // unique single column schema
        // the function is expected to return a single column which will be renamed to the column
        // name given
        var relName = anonymousRelName();
        var columnName = anonymousColumnName();
        var column = QueryAST.columnFromString(relName, columnName);
        var funcFrom = QueryAST.funcFrom(function, relName, List.of(column));
        setReturningFunctions.add(funcFrom);

        stack.push(column);
      } else if (modeStack.peek() == Placeholders.FROM_START) {
        // the set returning function is already in the from clause
        // create funcFrom and push to stack
        var relName = anonymousRelName();
        //        var columnName = anonymousColumnName();

        // we cannot yet assign a schema here, and need to wait until the exit from clause.
        var funcFrom = QueryAST.funcFrom(function, relName, List.of());

        stack.push(funcFrom);
      } else {
        throw new RuntimeException(functionName + " set returning function is not allowed here");
      }
    } else {
      stack.push(function);
    }
  }

  @Override
  public void exitTypeCastExpr(ExpressionSQLParserParser.TypeCastExprContext ctx) {
    assertStackItems(1);
    stack.push(
        QueryAST.function(
            "typeCast",
            List.of(
                QueryAST.constant(ctx.TYPE().getText()),
                assertIsType(QueryAST.Datum.class).apply(stack.pop()))));
  }

  @Override
  public void exitJsonGetExpr(ExpressionSQLParserParser.JsonGetExprContext ctx) {
    assertStackItems(1);
    Object key;
    if (ctx.NUMBER_LITERAL() != null) {
      try {
        key = Integer.parseInt(ctx.NUMBER_LITERAL().getText());
      } catch (NumberFormatException e) {
        throw new RuntimeException(
            "json get array indices must be whole integers and not "
                + ctx.NUMBER_LITERAL().getText());
      }
    } else {
      key = cleanStringLiteral(ctx.STRING_LITERAL().getText());
    }

    var operatorText = ctx.JSON_GET().getText();
    var functionName = operatorText.equals("->") ? "jsonGetToJson" : "jsonGetToString";
    stack.push(
        QueryAST.function(
            functionName,
            List.of(
                QueryAST.constant(key), assertIsType(QueryAST.Datum.class).apply(stack.pop()))));
  }

  @Override
  public void exitLimitClause(ExpressionSQLParserParser.LimitClauseContext ctx) {
    var q = queryStack.peek();
    if (q == null) throw new RuntimeException("query cannot be null after exiting a limit clause");

    q.setLimit(Optional.of(Integer.valueOf(ctx.limit.getText())));
  }

  /*
  Handle primitive types
  */

  @Override
  public void exitIdentifierDatum(ExpressionSQLParserParser.IdentifierDatumContext ctx) {
    // here we only expect single identifiers without a dot.
    // e.g a and "a.b" but not a.b
    // a.b will is handled in exitColumnSelection2
    var mode = modeStack.peek();
    var identifier = cleanIdentifier(ctx.IDENTIFIER().getText());

    if (mode == Placeholders.FROM_START || mode == Placeholders.JOIN_START) {
      stack.push(QueryAST.fromClauseIdentifier(identifier));
    } else {
      stack.push(QueryAST.columnFromString(null, identifier));
    }
  }

  @Override
  public void exitUnaryBooleanExprIsNotNull(
      ExpressionSQLParserParser.UnaryBooleanExprIsNotNullContext ctx) {
    assertStackItems(1);
    stack.push(QueryAST.isNotNull(assertIsType(QueryAST.Datum.class).apply(stack.pop())));
  }

  @Override
  public void exitUnaryBooleanExprIsNull(
      ExpressionSQLParserParser.UnaryBooleanExprIsNullContext ctx) {
    assertStackItems(1);
    stack.push(QueryAST.isNull(assertIsType(QueryAST.Datum.class).apply(stack.pop())));
  }

  @Override
  public void exitColumnSelection2(ExpressionSQLParserParser.ColumnSelection2Context ctx) {
    var identifier = cleanIdentifier(ctx.columnSelection().tableOrColumn.getText());
    if (ctx.columnSelection().column != null) {
      var column = ctx.columnSelection().column.getText();
      stack.push(QueryAST.columnFromString(identifier, column));
    } else {
      stack.push(QueryAST.columnFromString(null, identifier));
    }
  }

  @Override
  public void exitSelectTableReference2(
      ExpressionSQLParserParser.SelectTableReference2Context ctx) {
    assertStackItems(2);
    var from = stack.peek();
    if (from instanceof QueryAST.SubSelectFrom subSelect) {

      if (ctx.lateral != null) {
        // we have a lateral sub select
        stack.pop(); // remove it from the stack
        var q = queryStack.peek();
        if (q == null) {
          throw new RuntimeException("query expected here");
        }
        q.updateLateralFroms(List.of(subSelect));
      }
    }
  }

  @Override
  public void exitSelectTableReference(ExpressionSQLParserParser.SelectTableReferenceContext ctx) {
    // called when we exit a sub select in a from clause
    if (ctx.IDENTIFIER() != null) {
      var identifier = cleanIdentifier(ctx.IDENTIFIER().getText());
      if (identifier.isEmpty()) {
        throw new RuntimeException("empty identifiers are not allowed");
      }
      assertStackItems(1);
      var from = stack.peek();
      if (from instanceof QueryAST.SubSelectFrom subSelect) {
        if (Strings.isNullOrEmpty(subSelect.getName())) {
          subSelect.setName(identifier);
        } else {
          subSelect.setAlias(identifier);
        }
      }
    }
  }

  @Override
  public void exitStringDatum(ExpressionSQLParserParser.StringDatumContext ctx) {
    var txt = cleanStringLiteral(ctx.STRING_LITERAL().getText());
    stack.push(QueryAST.constant(txt));
  }

  @Override
  public void exitBooleanDatum(ExpressionSQLParserParser.BooleanDatumContext ctx) {
    stack.push(QueryAST.constant(ctx.TRUE() != null ? Boolean.TRUE : Boolean.FALSE));
  }

  @Override
  public void exitNumberDatum(ExpressionSQLParserParser.NumberDatumContext ctx) {
    var numberStr = ctx.getText();
    try {
      stack.push(QueryAST.constant(Long.valueOf(numberStr)));
    } catch (NumberFormatException nfe) {
      try {
        stack.push(QueryAST.constant(Double.valueOf(numberStr)));
      } catch (NumberFormatException nfe2) {
        stack.push(QueryAST.constant(new BigDecimal(numberStr)));
      }
    }
  }

  @Override
  public void exitNullDatum(ExpressionSQLParserParser.NullDatumContext ctx) {
    stack.push(QueryAST.NULL);
  }

  @Override
  public void enterCaseWhenExpr(ExpressionSQLParserParser.CaseWhenExprContext ctx) {
    modeStack.push(Placeholders.CASE_WHEN_START);
    stack.push(Placeholders.CASE_WHEN_START);
  }

  @Override
  public void enterWhenExprCondition(ExpressionSQLParserParser.WhenExprConditionContext ctx) {
    modeStack.push(Placeholders.CASE_WHEN_CONDITION_START);
    stack.push(Placeholders.CASE_WHEN_CONDITION_START);
  }

  @Override
  public void exitWhenExprCondition(ExpressionSQLParserParser.WhenExprConditionContext ctx) {
    modeStack.pop();
    List<QueryAST.Datum> args =
        popToMarkerAndReverse(
            Placeholders.CASE_WHEN_CONDITION_START, assertIsType(QueryAST.Datum.class));
    if (args.isEmpty()) {
      stack.push(QueryAST.constant(true));
    } else if (args.size() == 1) {
      stack.push(args.get(0));
    } else {
      var left =
          QueryAST.binaryBoolExpr(args.get(0), args.get(1), QueryAST.BinaryBooleanOperator.AND);
      var op =
          ctx.condition.getText().equalsIgnoreCase("and")
              ? QueryAST.BinaryBooleanOperator.AND
              : QueryAST.BinaryBooleanOperator.OR;
      for (int i = 2; i < args.size(); i++) {
        left = QueryAST.binaryBoolExpr(left, args.get(i), op);
      }
      stack.push(left);
    }
  }

  @Override
  public void exitWhenExpr(ExpressionSQLParserParser.WhenExprContext ctx) {
    // we expect the when test and the then clause
    assertStackItems(2);
    var result = assertIsType(QueryAST.Datum.class).apply(stack.pop());
    var whenTest = assertIsType(QueryAST.Datum.class).apply(stack.pop());

    stack.push(new QueryAST.WhenExpr(whenTest, result));
  }

  @Override
  public void exitCaseWhenExpr(ExpressionSQLParserParser.CaseWhenExprContext ctx) {
    modeStack.pop();
    List<QueryAST.Datum> args =
        popToMarkerAndReverse(Placeholders.CASE_WHEN_START, assertIsType(QueryAST.Datum.class));

    List<QueryAST.WhenExpr> whenClauses = new ArrayList<>();
    QueryAST.Datum defaultClause = null;

    for (var datum : args) {
      if (datum instanceof QueryAST.WhenExpr whenExpr) {
        whenClauses.add(whenExpr);
      } else {
        defaultClause = datum;
      }
    }

    stack.push(new QueryAST.CaseWhenExpr(whenClauses, new QueryAST.WhenElseExpr(defaultClause)));
  }

  /** Support for the Mark Gather technique. See QueryAST header for explanation. */
  @SuppressWarnings("unchecked")
  private <T, R> List<R> popToMarkerAndReverse(Placeholders marker, Function<T, R> predicate) {
    List<R> args = new ArrayList<>();
    Object stackObj;
    assertStackItems(1);

    while ((stackObj = stack.pop()) != marker) {
      args.add(predicate.apply((T) stackObj));
    }
    Collections.reverse(args);
    return args;
  }

  private static <T> Function<Object, T> assertIsType(Class<T> cls) {
    return (obj) -> {
      if (!cls.isInstance(obj)) {
        throw new RuntimeException(
            "expecting a type " + cls + " expression on stack but found; " + obj);
      }
      return cls.cast(obj);
    };
  }

  private void assertStackItems(int i) {
    if (stack.size() < i) {
      throw new RuntimeException(
          "internal issue; we expect at least "
              + i
              + " elements on the stack but only "
              + stack.size()
              + " found");
    }
  }

  private void pushBinaryBooleanExpr(String op) {
    var right = stack.pop();
    var left = stack.pop();

    stack.push(
        QueryAST.binaryBoolExpr(
            assertIsType(QueryAST.Datum.class).apply(left),
            assertIsType(QueryAST.Datum.class).apply(right),
            QueryAST.BinaryBooleanOperator.fromString(op.toUpperCase())));
  }

  /**
   * Transforms a literal string concat into a concat function call. Example:
   *
   * <pre>`select 'a' || 'b'` is seen as `select concat('a', 'b')`</pre>
   *
   * .
   */
  private void pushStringLiteralConcatToConcatFunctionTransform() {
    var right = stack.pop();
    var left = stack.pop();

    stack.push(
        QueryAST.function(
            Concat.NAME,
            List.of(
                assertIsType(QueryAST.Datum.class).apply(left),
                assertIsType(QueryAST.Datum.class).apply(right))));
  }

  private void pushBinaryArithmeticExpr(String op) {
    var right = stack.pop();
    var left = stack.pop();

    stack.push(
        QueryAST.binaryAritmeticExpr(
            assertIsType(QueryAST.Datum.class).apply(left),
            assertIsType(QueryAST.Datum.class).apply(right),
            QueryAST.BinaryArithmeticOperator.fromString(op.toUpperCase())));
  }

  private void pushUnaryBooleanExpr(String op) {
    var expr = stack.pop();

    stack.push(
        QueryAST.unaryBooleanExpr(
            assertIsType(QueryAST.Datum.class).apply(expr),
            QueryAST.UnaryBooleanOperator.fromString(op.toUpperCase())));
  }

  private String anonymousRelName() {
    return "rel_" + anonymousRelNameCounter.incrementAndGet();
  }

  private String anonymousColumnName() {
    return "col_" + anonymousColumnNameCounter.incrementAndGet();
  }
}
