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
import io.fleak.zephflow.lib.sql.ast.QueryASTParser;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystems;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryASTInterpreterTest {

  @Test
  public void testSpecialCharactersInIdentifiers() {

      var typeSystem = TypeSystems.sqlTypeSystem();
      var interpreter = Interpreter.astInterpreter(typeSystem);

    var testData =
        new Object[][] {
          {"\"name@$%^\"", "name@$%^"},
          {"\"This string has an escaped quote: \\\"\"", "This string has an escaped quote: \\\""},
          {"\"Special characters: !@#$%^&*()_+\"", "Special characters: !@#$%^&*()_+"},
          {
            "\"Unicode characters: 你好, мир, \uD83D\uDE0A\"",
            "Unicode characters: 你好, мир, \uD83D\uDE0A"
          }
        };

      for(int i =0; i < testData.length; i++) {
          var testItem = testData[i];
          System.out.println("Testing [" + i + "]: " + Arrays.toString(testItem));
          var identifierName = testItem[0].toString();
          var recordFieldName = testItem[1];

          QueryAST.Query query =
                  QueryASTParser.astParser().parseSelectStatement("select " + identifierName + " from events");

          var inputMap = new HashMap<String, Object>(); // don't use Map.of, we need to support null values
          inputMap.put(recordFieldName.toString(), 1L);
          var table = Table.ofMap(TypeSystems.sqlTypeSystem(), "events", inputMap);
          var expr = query.getColumns().get(0);
          var v = interpreter.eval(table.iterator().next(), expr, Object.class);

          Assertions.assertEquals(v, 1L);

      }
  }


  @Test
  public void testBooleanCasts() {
    var typeSystem = TypeSystems.sqlTypeSystem();
    var interpreter = Interpreter.astInterpreter(typeSystem);

      Object[][] testData =
              new Object[][] {
                      {"true", true},
                      {"t", true},
                      {"yes", true},
                      {"y", true},
                      {"1", true},
                      {"on", true},
                      {"false", false},
                      {"f", false},
                      {"no", false},
                      {"n", false},
                      {"0", false},
                      {"off", false},
                      {"2", "error"},
                      {"maybe", "error"},
                      {"foo", "error"},
                      {null, false},
                      {"-1", "error"},
                      {"123", "error"},
                      {"0.0", "error"},
                      {"1.0", "error"},
                      {"-0.0", "error"},
                      {"42", "error"},
                      {"3.14", "error"},
                      {true, true},
                      {false, false},
                      {1, true},
                      {0, false},
                      {-1, true},
                      {-123, true},
                      {123, true},
                      {0.0, "error"},
                      {1.0, "error"},
                      {-0.0, "error"},
                      {42, true},
                      {3.14, "error"}
              };

    for (int i = 0; i < testData.length; i++) {
      var testItem = testData[i];
      System.out.println("Testing item [" + i + "] => " + Arrays.toString(testItem));

      var val = testItem[0];
      var expected = testItem[1];

        QueryAST.Query query =
                QueryASTParser.astParser().parseSelectStatement("select a::bool from events");

        var inputMap = new HashMap<String, Object>(); // don't use Map.of, we need to support null values
        inputMap.put("a", val);
        var table = Table.ofMap(TypeSystems.sqlTypeSystem(), "events", inputMap);
        var expr = query.getColumns().get(0);
        try{
        var v = interpreter.eval(table.iterator().next(), expr, Object.class);
        Assertions.assertTrue(v instanceof Boolean);

        Assertions.assertEquals(((Boolean)expected).booleanValue(), ((Boolean) v).booleanValue());

        } catch (RuntimeException rte) {
            if(expected.equals("error")){
                Assertions.assertTrue(rte.toString().contains("cast"));
            } else {
                Assertions.fail("Expected a cast exception but got " + rte);
            }

        }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInterpretExpressions() {

    Object[][] testData =
        new Object[][] {
          {Map.of("a", Map.of("b", 100)), "select a->'b' from input", 100},
          {
            Map.of("a", Map.of("b", Map.of("c", 2)), "c", 3), "select a->'b'->'c' + c from input", 5
          },
          {Map.of("a", List.of(10, 11, 12)), "select a->1 from input", 11},
          {Map.of("a", List.of(1, Map.of("z", 10))), "select a->0 + a->1->'z' from input", 11},
          {Map.of("a", 1, "b", 2), "select a+b from input", 3},
          {Map.of("a", 2, "b", 2), "select a*b from input", 4},
          {Map.of("a", 1, "b", 2), "select a+b from input", 3},
          {Map.of("column1", 5, "column2", 10), "select column1 + column2 from input", 15},
          {
            Map.of("column1", 10, "column2", 5, "column3", 8),
            "select column1 * column2 + column3 from input",
            58
          },
          {
            Map.of("column1", 15, "column2", 7, "column3", 3, "column4", 20),
            "select (column1 / column2) + (column3 - column4) from input",
            -15
          },
          {
            Map.of("column1", 20, "column2", 10, "column3", 4, "column4", 3, "column5", 6),
            "select column1 * column2 / (column3 ^ column4) + column5 from input",
            9
          },
          {
            Map.of(
                "column1", 25, "column2", 12, "column3", 6, "column4", 2, "column5", 3, "column6",
                4),
            "select (column1 + column2) * (column3 - column4) / column5 % column6 from input",
            1
          },
          {
            Map.of(
                "column1", 30, "column2", 15, "column3", 5, "column4", 3, "column5", 2, "column6",
                6, "column7", 8),
            "select (column1 + column2 - column3 * column4 / (column5 ^ column6))::int % column7 from input",
            5
          },
          {
            Map.of(
                "column1", 35, "column2", 18, "column3", 9, "column4", 3, "column5", 2, "column6",
                7, "column7", 5, "column8", 10),
            "select column1 * column2 / (column3 - column4) + (column5 ^ column6) - column7 % column8 from input",
            228
          },
          {
            Map.of(
                "column1", 40, "column2", 20, "column3", 10, "column4", 5, "column5", 2, "column6",
                6, "column7", 3, "column8", 8, "column9", 12),
            "select (column1 / (column2 + column3) * (column4 - column5) + (column6 ^ column7))::int % column8 - column9 from input",
            -9
          },
          {
            Map.of(
                "column1",
                45,
                "column2",
                22,
                "column3",
                11,
                "column4",
                4,
                "column5",
                3,
                "column6",
                7,
                "column7",
                2,
                "column8",
                9,
                "column9",
                15,
                "column10",
                6),
            "select column1 + (column2 - column3) * (column4 / column5) ^ (column6 % column7) + column8 - (column9 * column10) from input",
            -25
          },
          {Map.of("a", "1"), "select a || 'b' || 'c' from input", "1bc"},
          {Map.of("a", "abc", "b", 2), "select a like 'a%' from input", true},
          {Map.of("a", "abc", "b", 2), "select a like 'a' || '%' || 'c' from input", true},
          {Map.of("a", "abc", "b", 2), "select a like 'a' || 'd%' from input", false},
          {Map.of("a", "abc", "b", 2), "select a like 'A%' from input", false},
          {Map.of("a", "abc", "b", 2), "select a ilike 'A%' from input", true},
          {Map.of("a", "abc", "b", 2), "select a ilike 'ABC' from input", true},
        };

    var typeSystem = TypeSystems.sqlTypeSystem();
    var interpreter = Interpreter.astInterpreter(typeSystem);

    for (int i = 0; i < testData.length; i++) {
      System.out.println("Testing item [" + i + "]");

      var testItem = testData[i];
      Map<String, Object> input = (Map<String, Object>) testItem[0];
      QueryAST.Query query =
          QueryASTParser.astParser().parseSelectStatement(testItem[1].toString());
      Object expected = testItem[2];
      var table = Table.ofMap(TypeSystems.sqlTypeSystem(), "input", input);

      var expr = query.getColumns().get(0);

      var v = interpreter.eval(table.iterator().next(), expr, Object.class);

      var same = typeSystem.lookupTypeBoolean(expected.getClass()).equal(expected, v);
      System.out.println();
      Assertions.assertTrue(same, "expect[ " + expected + " ] != given[ " + v + " ]");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInterpretBooleanWhereExpressions() {

    Object[][] testData =
        new Object[][] {
          new Object[] {
            Map.of("column1", 1, "column2", 2), "select 1 from input where column1 > column2", false
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2), "select 1 from input where column1 < column2", true
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2),
            "select 1 from input where column1 >= column2",
            false
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2), "select 1 from input where column1 <= column2", true
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2), "select 1 from input where column1 = column2", false
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2), "select 1 from input where column1 != column2", true
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2),
            "select 1 from input where column1 > 0 AND column2 < 3",
            true
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2),
            "select 1 from input where column1 < 0 OR column2 > 3",
            false
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2),
            "select 1 from input where (column1 = 1 AND column2 = 2) OR column1 = 0",
            true
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2),
            "select 1 from input where (column1 != 1 OR column2 != 2) AND column1 = 1",
            false
          },
          new Object[] {
            Map.of("column1", 1.0, "column2", 2.5),
            "select 1 from input where column1 > column2",
            false
          },
          new Object[] {
            Map.of("column1", 1.0, "column2", 2.5),
            "select 1 from input where column1 < column2",
            true
          },
          new Object[] {
            Map.of("column1", 1.0, "column2", 2.5),
            "select 1 from input where column1 >= column2",
            false
          },
          new Object[] {
            Map.of("column1", 1.0, "column2", 2.5),
            "select 1 from input where column1 <= column2",
            true
          },
          new Object[] {
            Map.of("column1", 1.0, "column2", 2.5),
            "select 1 from input where column1 = column2",
            false
          },
          new Object[] {
            Map.of("column1", 1.0, "column2", 2.5),
            "select 1 from input where column1 != column2",
            true
          },
          new Object[] {
            Map.of("column1", 1.0, "column2", 2.5),
            "select 1 from input where column1 > 0 AND column2 < 3",
            true
          },
          new Object[] {
            Map.of("column1", 1.0, "column2", 2.5),
            "select 1 from input where column1 < 0 OR column2 > 3",
            false
          },
          new Object[] {
            Map.of("column1", 1.0, "column2", 2.5),
            "select 1 from input where (column1 = 1.0 AND column2 = 2.5) OR column1 = 0",
            true
          },
          new Object[] {
            Map.of("column1", 1.0, "column2", 2.5),
            "select 1 from input where (column1 != 1.0 OR column2 != 2.5) AND column1 = 1.0",
            false
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2),
            "select 1 from input where column1 = 1 AND column2 = 2 OR column1 = 0",
            true
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2),
            "select 1 from input where (column1 = 1 AND column2 = 2) OR column1 = 0",
            true
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2),
            "select 1 from input where column1 = 1 AND (column2 = 2 OR column1 = 0)",
            true
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2),
            "select 1 from input where column1 = 1 OR column2 = 2 AND column1 = 0",
            false
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2),
            "select 1 from input where (column1 = 1 OR column2 = 2) AND column1 = 0",
            false
          },
          new Object[] {
            Map.of("column1", 1, "column2", 2),
            "select 1 from input where column1 = 1 OR (column2 = 2 AND column1 = 0)",
            true
          },
          new Object[] {Map.of(), "select 1 from input where 2*2+3 = 7", true},
          new Object[] {Map.of(), "select 1 from input where 2*(2+3) = 7", false},
        };

    var typeSystem = TypeSystems.sqlTypeSystem();
    var interpreter = Interpreter.astInterpreter(typeSystem);

    for (int i = 0; i < testData.length; i++) {
      System.out.println("Testing item [" + i + "]");

      var testItem = testData[i];
      Map<String, Object> input = (Map<String, Object>) testItem[0];
      QueryAST.Query query =
          QueryASTParser.astParser().parseSelectStatement(testItem[1].toString());
      Object expected = testItem[2];
      var table = Table.ofMap(TypeSystems.sqlTypeSystem(), "input", input);

      var expr = query.getWhere();

      var v = interpreter.eval(table.iterator().next(), expr, Object.class);

      var same = typeSystem.lookupTypeBoolean(expected.getClass()).equal(expected, v);

      Assertions.assertTrue(same, "expect[ " + expected + " ] != given[ " + v + " ]");
    }
  }
}
