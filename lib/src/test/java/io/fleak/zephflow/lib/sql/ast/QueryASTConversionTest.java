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

import io.fleak.zephflow.lib.sql.TestResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QueryASTConversionTest {

    /**
     * @TOTO test parse power function
     */
    @Test
    public void testSimpleQueryConversion() {

        var listener = ParserHelper.selectStatementToAST("select json_array_elements(matches1) -> 'metadata' from test345");
        System.out.println(listener.getInnerQuery().toString());

        var expected = "QueryAST.Query(from=[SimpleFrom{name='input', alias='null'}, SubSelectFrom{query=QueryAST.Query(from=null, where=null, having=null, columns=[QueryAST.Constant(value=1)], name=null), name='null', alias='null'}], where=BinaryBooleanExpr{left=SimpleColumn{name='a', relName='null', alias='null'}, right=QueryAST.Constant(value=1), op=>, name='null', alias='null'}, having=null, columns=[SimpleColumn{name='a', relName='null', alias='null'}], name=null)";
        //QueryAST.Query(from=null, where=null, having=null, columns=[FuncCall{functionName='typeCast', args=[QueryAST.Constant(value=date), SimpleColumn{name='a', relName='null', alias='null'}], name='null', alias='null'}, FuncCall{functionName='typeCast', args=[QueryAST.Constant(value=date), QueryAST.Constant(value='2024-01-01')], name='null', alias='null'}, FuncCall{functionName='count', args=[SimpleColumn{name='id', relName='i', alias='null'}], name='null', alias='val'}], name=null)
        Assertions.assertNotNull(listener);
    }

    @Test
    public void testQueryConversion() {
        var testQueries = TestResources.getSqlAsAstQueries();
        var queries = testQueries.get("queries");
        int line = 0;
        for(var q : queries){
            line++;
            try {
                var listener = ParserHelper.selectStatementToAST(q);
                Assertions.assertNotNull(listener);

            } catch (RuntimeException e) {
                System.out.println("Error at " + line + " q: " + q);
                throw e;
            }

        }
    }



}
