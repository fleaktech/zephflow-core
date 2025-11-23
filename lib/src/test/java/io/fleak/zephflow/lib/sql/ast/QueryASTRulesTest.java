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

import com.google.common.base.Strings;
import io.fleak.zephflow.lib.sql.TestResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueryASTRulesTest {

  @Test
  public void testJoinLateralInSelect() {
    var listener =
        ParserHelper.selectStatementToAST(
            "SELECT json_array_elements(articles) ->> 'text' AS \"text\" FROM event;");
    var queryAst = QueryASTRules.apply(listener.getInnerQuery());
    Assertions.assertTrue(queryAst.getLateralFroms().size() == 1);
    Assertions.assertTrue(queryAst.getLateralFroms().get(0).getAliasOrName().equals("rel_1"));
    var cols = queryAst.getColumns();
    var col = cols.iterator().next();
    Assertions.assertTrue(col instanceof QueryAST.FuncCall);
    var fnCall = (QueryAST.FuncCall) col;
    Assertions.assertTrue(fnCall.getArgs().size() == 2);
    Assertions.assertEquals(fnCall.getAlias(), "text");
    var argTwo = fnCall.getArgs().get(1);
    Assertions.assertTrue(argTwo instanceof QueryAST.SimpleColumn);
    var col2 = (QueryAST.SimpleColumn) argTwo;
    Assertions.assertEquals(col2.getRelName(), "rel_1");
    Assertions.assertEquals(col2.getName(), "col_1");
  }

  @Test
  public void testJoinLateralInFromClause() {
    var listener =
        ParserHelper.selectStatementToAST(
            "SELECT article ->> 'text' AS \"text\" FROM event, json_array_elements(articles) AS article;");
    var queryAst = QueryASTRules.apply(listener.getInnerQuery());
    Assertions.assertTrue(queryAst.getLateralFroms().size() == 1);
    Assertions.assertTrue(queryAst.getLateralFroms().get(0).getAliasOrName().equals("article"));
    var cols = queryAst.getColumns();
    var col = cols.iterator().next();
    Assertions.assertTrue(col instanceof QueryAST.FuncCall);
    var fnCall = (QueryAST.FuncCall) col;
    Assertions.assertTrue(fnCall.getArgs().size() == 2);
    Assertions.assertEquals(fnCall.getAlias(), "text");
    var argTwo = fnCall.getArgs().get(1);
    Assertions.assertTrue(argTwo instanceof QueryAST.SimpleColumn);
    var col2 = (QueryAST.SimpleColumn) argTwo;
    Assertions.assertEquals(col2.getRelName(), "article");
    Assertions.assertEquals(col2.getName(), "article");
  }

  @Test
  public void testRenameRules() {
    var testResources = TestResources.getSqlAsAstQueries();
    var queries = testResources.get("queries");
    for (var query : queries) {
      System.out.println(query);
      var listener = ParserHelper.selectStatementToAST(query);

      var queryAst = QueryASTRules.apply(listener.getInnerQuery());

      queryAst.walk(
          datum -> {
            if (datum instanceof QueryAST.SimpleColumn) {
              var relName = ((QueryAST.SimpleColumn) datum).getRelName();
              Assertions.assertFalse(
                  Strings.isNullOrEmpty(relName), "Rel name cannot be null for " + datum);
            }
          });
    }
  }
}
