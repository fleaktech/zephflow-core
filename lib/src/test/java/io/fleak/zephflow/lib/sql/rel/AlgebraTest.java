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
package io.fleak.zephflow.lib.sql.rel;

import static io.fleak.zephflow.lib.sql.rel.Algebra.*;

import io.fleak.zephflow.lib.sql.ast.QueryAST;
import io.fleak.zephflow.lib.sql.ast.QueryASTParser;
import io.fleak.zephflow.lib.sql.exec.functions.JsonGetToJson;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AlgebraTest {

  @Test
  public void testSubSelectInFromAlgebra() {

    var sql = "select * from (select * from records) e;";

    var translation =
        QueryTranslator.translate(QueryASTParser.astParser().parseSelectStatement(sql));

    Assertions.assertNotNull(translation);
  }

  @Test
  public void testAlgebra() {
    var testData =
        new Object[][] {
          {
            "select table.a, table.b from table;",
            Algebras.project(
                Algebras.table("table"),
                List.of(Algebras.column("table", "a", 0), Algebras.column("table", "b", 1))),
          },
          {
            "select table.a as z, table.b from table",
            Algebras.rename(
                Algebras.project(
                    Algebras.table("table"),
                    List.of(
                        Algebras.column(QueryAST.columnFromString("table", "a", "z"), 0),
                        Algebras.column("table", "b", 1))),
                List.of(
                    Algebras.renameField(
                        Algebras.column(QueryAST.columnFromString("table", "a", "z")), "z")))
          },
          {
            "select t.a as c1, t.b as c2 from table as t",
            Algebras.rename(
                Algebras.project(
                    Algebras.table("table", "t"),
                    List.of(
                        Algebras.column(QueryAST.columnFromString("t", "a", "c1")),
                        Algebras.column(QueryAST.columnFromString("t", "b", "c2")))),
                List.of(
                    Algebras.column(QueryAST.columnFromString("t", "a", "c1")).asRenameField("c1"),
                    Algebras.column(QueryAST.columnFromString("t", "b", "c2")).asRenameField("c2")))
          },
          {
            "select a, 1*2 as c from table",
            Algebras.project(
                Algebras.extend(
                    Algebras.table("table"),
                    "c",
                    Algebras.astExpr(
                        QueryAST.binaryAritmeticExpr(
                            QueryAST.constant(1),
                            QueryAST.constant(2),
                            QueryAST.BinaryArithmeticOperator.MULTIPLY,
                            "col_1",
                            "c")),
                    1),
                List.of(Algebras.column("table", "a", 0), Algebras.column("table", "c", 1)))
          },
          {
            "select table1.a, table2.a from table1, table2",
            Algebras.project(
                Algebras.crossProduct(
                    List.of(Algebras.table("table1"), Algebras.table("table2")), "c"),
                List.of(Algebras.column("table1", "a", 0), Algebras.column("table2", "a", 1)))
          },
          {
            "select table.a, table.b from table where table.a > 1",
            Algebras.project(
                Algebras.restrict(
                    Algebras.table("table"),
                    Algebras.astExpr(
                        QueryAST.binaryBoolExpr(
                            QueryAST.columnFromString("table", "a"),
                            QueryAST.constant(1),
                            QueryAST.BinaryBooleanOperator.GREATER_THAN))),
                List.of(Algebras.column("table", "a", 0), Algebras.column("table", "b", 1)))
          },
          {
            "select table1.a, table2.a from table1, table2 where table1.a != table2.a",
            Algebras.project(
                Algebras.restrict(
                    Algebras.crossProduct(
                        List.of(Algebras.table("table1"), Algebras.table("table2")), "_"),
                    Algebras.astExpr(
                        QueryAST.binaryBoolExpr(
                            QueryAST.columnFromString("table1", "a"),
                            QueryAST.columnFromString("table2", "a"),
                            QueryAST.BinaryBooleanOperator.NOT_EQUAL))),
                List.of(
                    Algebras.column(QueryAST.columnFromString("table1", "a")),
                    Algebras.column(QueryAST.columnFromString("table2", "a"))))
          },
          {
            "select t.a from (select s.a from s) t",
            Algebras.project(
                Algebras.view(
                    Algebras.project(Algebras.table("s"), List.of(Algebras.column("s", "a", 0))),
                    "t"),
                List.of(Algebras.column("t", "a", 0)))
          },
          {
            "select t.a, r.b from r, (select s.a from s) t",
            Algebras.project(
                Algebras.crossProduct(
                    List.of(
                        Algebras.table("r"),
                        Algebras.view(
                            Algebras.project(
                                Algebras.table("s"), List.of(Algebras.column("s", "a", 0))),
                            "t")),
                    "u"),
                List.of(Algebras.column("t", "a", 0), Algebras.column("r", "b", 1)))
          },
          {
            "select t.a, count(t.a) b from t group by t.a",
            Algebras.rename(
                Algebras.project(
                    Algebras.aggregate(
                        "t",
                        Algebras.group(
                            Algebras.table("t"),
                            List.of(Algebras.column("t", "a", -1)),
                            "t",
                            List.of()),
                        List.of(
                            Algebras.aggregateFunc(
                                QueryAST.function(
                                    "count",
                                    List.of(QueryAST.columnFromString("t", "a", "")),
                                    "col_1",
                                    null),
                                "t")),
                        -1),
                    List.of(Algebras.column("t", "a", 0), Algebras.column("t", "col_1", 0, "a"))),
                List.of(Algebras.renameField(Algebras.column("col_1", "t", 0, "b"), "b")))
          },
          {
            "select t.a from t limit 10",
            Algebras.limit(
                Algebras.project(Algebras.table("t"), List.of(Algebras.column("t", "a", 0))), 10)
          },
          //          {
          //            "select distinct t.a from t",
          //            Algebras.distinct(
          //                Algebras.project(Algebras.table("t"), List.of(Algebras.column("t", "a",
          // 0))))
          //          },
          {
            "select * from t",
            Algebras.project(
                Algebras.table("t"), List.of(Algebras.column(QueryAST.allColumn(null))))
          },
          {
            "select a->'b' from t",
            Algebras.project(
                Algebras.extend(
                    Algebras.table("t"),
                    "t",
                    Algebras.astExpr(
                        QueryAST.function(
                            JsonGetToJson.NAME,
                            List.of(QueryAST.constant("b"), QueryAST.columnFromString("t", "a")))),
                    0),
                List.of(Algebras.column("t", "col_1", 0)))
          },
          {
            "select 1+2 as v from t",
            Algebras.project(
                Algebras.extend(
                    Algebras.table("t"),
                    "v",
                    Algebras.astExpr(
                        QueryAST.binaryAritmeticExpr(
                            QueryAST.constant(1),
                            QueryAST.constant(2),
                            QueryAST.BinaryArithmeticOperator.ADD,
                            "col_1",
                            "v")),
                    0),
                List.of(Algebras.column("t", "v", 0)))
          },
          {
            "select 1+2 as v, a from t",
            Algebras.project(
                Algebras.extend(
                    Algebras.table("t"),
                    "v",
                    Algebras.astExpr(
                        QueryAST.binaryAritmeticExpr(
                            QueryAST.constant(1),
                            QueryAST.constant(2),
                            QueryAST.BinaryArithmeticOperator.ADD,
                            "col_1",
                            "v")),
                    0),
                List.of(Algebras.column("t", "v", 0), Algebras.column("t", "a", 1)))
          },
          //                {
          //                    "SELECT b.b, string_agg(event.a, ',') from event,
          // json_array_elements(event.b) b group by b.b",
          //                        Algebras.table("test")
          //                }
          //          ,
          //          {
          //            "SELECT article ->> 'text' AS \"text\" FROM event,
          // json_array_elements(articles) AS article;",
          //            Algebras.table("test")
          //          }
        };

    for (int i = 0; i < testData.length; i++) {
      var data = testData[i];
      var sql = data[0].toString();
      var expected = (Relation) data[1];

      System.out.println("Testing [" + i + "]: " + sql);

      var translation =
          QueryTranslator.translate(QueryASTParser.astParser().parseSelectStatement(sql));
      Assertions.assertNotNull(expected);
      Assertions.assertNotNull(translation, "translation cannot be null here: " + translation);

      assertEquals(expected, translation);
    }
  }

  private void assertEquals(Relation expected, Relation translation) {
    System.out.println("expected: " + expected);
    System.out.println("translation: " + translation);

    Assertions.assertEquals(expected, translation);
  }
}
