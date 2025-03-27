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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.fleak.zephflow.lib.sql.TestResources;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParserHelperTest {

    @Test
    public void runBooleanTestCases() {
        /*
         testing to see that the parser correctly maintains boolean associativity
         i.e for 1 < 2 and 2 > 1 we can never see an expression tree with

         */

        var testData = new Object[][]{
                {
                        "select 1 from input where 1 > 0 AND 2 < 3",
                        QueryAST.binaryBoolExpr(
                                QueryAST.binaryBoolExpr(QueryAST.constant(1), QueryAST.constant(0), QueryAST.BinaryBooleanOperator.GREATER_THAN),
                                QueryAST.binaryBoolExpr(QueryAST.constant(2), QueryAST.constant(3), QueryAST.BinaryBooleanOperator.LESS_THAN),
                                QueryAST.BinaryBooleanOperator.AND
                        )
                },
                {
                        "select 1 from input where 1 > 0 AND 2 < 3 or 1 < 2",
                        QueryAST.binaryBoolExpr(
                                QueryAST.binaryBoolExpr(
                                        QueryAST.binaryBoolExpr(QueryAST.constant(1), QueryAST.constant(0), QueryAST.BinaryBooleanOperator.GREATER_THAN),
                                        QueryAST.binaryBoolExpr(QueryAST.constant(2), QueryAST.constant(3), QueryAST.BinaryBooleanOperator.LESS_THAN),
                                        QueryAST.BinaryBooleanOperator.AND
                                ),
                                QueryAST.binaryBoolExpr(
                                        QueryAST.constant(1),
                                        QueryAST.constant(2),
                                        QueryAST.BinaryBooleanOperator.LESS_THAN
                                ),
                                QueryAST.BinaryBooleanOperator.OR
                        )
                },

        };
      for (Object[] testDatum : testData) {
        var q = testDatum[0].toString();
        var expected = testDatum[1];
        var listener = ParserHelper.selectStatementToAST(q);
        var whereExpr = listener.getInnerQuery().getWhere();
        Assertions.assertEquals(expected, whereExpr);
      }

    }

    @Test
    public void runSelectTestCases() {
        var testQueries = TestResources.getTestQueries();
        var queries = testQueries.get("queries");

        for (Map.Entry<String, List<String>> entry : queries.entrySet()) {
            var expressionName = entry.getKey();

            for (var sqlQuery : entry.getValue()) {
                System.out.println(sqlQuery);
                if (expressionName.equalsIgnoreCase("selectStatement")) {
                    var result = ParserHelper.runSelectStatementParser(sqlQuery);
                    assertNotNull(result);
                } else if (expressionName.equalsIgnoreCase("expr")) {
                    var result = ParserHelper.runExprStatement(sqlQuery);
                    assertNotNull(result);
                } else {
                    throw new RuntimeException(expressionName + " is not supported");
                }
            }

        }
    }

}
