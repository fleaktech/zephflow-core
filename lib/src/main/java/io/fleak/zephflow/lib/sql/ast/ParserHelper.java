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

import static io.fleak.zephflow.lib.utils.AntlrUtils.ensureConsumedAllTokens;
import static io.fleak.zephflow.lib.utils.AntlrUtils.parseInput;

import io.fleak.zephflow.lib.antlr.ExpressionSQLParserParser;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

public class ParserHelper {

  public static ExpressionSQLParserParser.ExprContext runExprStatement(String expr) {
    expr = prepareExpressionString(expr);

    ExpressionSQLParserParser parser =
        (ExpressionSQLParserParser) parseInput(expr, AntlrUtils.GrammarType.EXPRESSION_SQL);

    var result = parser.expr();
    if (result.exception != null) {
      throw new RuntimeException("error parsing " + expr + "; " + result.exception);
    }
    return result;
  }

  public static ExpressionToASTListener selectStatementToAST(String sqlStatement) {
    sqlStatement = prepareExpressionString(sqlStatement);
    ExpressionSQLParserParser parser =
        (ExpressionSQLParserParser) parseInput(sqlStatement, AntlrUtils.GrammarType.EXPRESSION_SQL);

    var listener = new ExpressionToASTListener();
    ParseTreeWalker.DEFAULT.walk(listener, parser.selectStatement());
    ensureConsumedAllTokens(parser);
    return listener;
  }

  public static ExpressionSQLParserParser.SelectStatementContext runSelectStatementParser(
      String sqlStatement) {
    sqlStatement = prepareExpressionString(sqlStatement);
    ExpressionSQLParserParser parser =
        (ExpressionSQLParserParser) parseInput(sqlStatement, AntlrUtils.GrammarType.EXPRESSION_SQL);

    var result = parser.selectStatement();
    if (result.exception != null) {
      throw new RuntimeException("error parsing " + sqlStatement + "; " + result.exception);
    }
    return result;
  }

  private static String prepareExpressionString(String expr) {
    expr = expr.trim();
    if (!expr.endsWith(";")) {
      expr = expr + ";";
    }
    return expr;
  }
}
