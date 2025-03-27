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
import io.fleak.zephflow.lib.sql.exec.utils.Streams;
import io.fleak.zephflow.lib.sql.rel.QueryTranslator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class RelationInterpreterTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testRelationalInterpreter() {
    var typeSystem = TypeSystems.sqlTypeSystem();

    var testData =
        new Object[][] {
                {
                        "select a, b, count(*) as c from event group by a, b",
                        Map.of("a", 1, "b", 2, "c", 3),
                        new Row.Entry[] {
                                Row.asEntry(Row.asKey("event", "a", 0), Row.asValue(1)),
                                Row.asEntry(Row.asKey("event", "b", 1), Row.asValue(2)),
                                Row.asEntry(Row.asKey("event", "c", 1), Row.asValue(1))

                        }
                },
          {
            "select a, b from event",
            Map.of("a", 1, "b", 2, "c", 3),
            new Row.Entry[] {
              Row.asEntry(Row.asKey("event", "a", 0), Row.asValue(1)),
              Row.asEntry(Row.asKey("event", "b", 1), Row.asValue(2))
            }
          },
          {
            "select event.a as z, event.b from event",
            Map.of("a", 1, "b", 2, "c", 3),
            new Row.Entry[] {
              Row.asEntry(Row.asKey("event", "z", 0), Row.asValue(1)),
              Row.asEntry(Row.asKey("event", "b", 1), Row.asValue(2))
            }
          },
          {
            "select t.a as c1, t.b as c2 from event as t",
            Map.of("a", 1, "b", 2, "c", 3),
            new Row.Entry[] {
              Row.asEntry(Row.asKey("t", "c1", 0), Row.asValue(1)),
              Row.asEntry(Row.asKey("t", "c2", 1), Row.asValue(2))
            }
          },
          {
            "select a, 2*2 as c from event",
            Map.of("a", 1, "b", 2, "c", 3),
            new Row.Entry[] {
              Row.asEntry(Row.asKey("event", "a", 0), Row.asValue(1)),
              Row.asEntry(Row.asKey("event", "c", 1), Row.asValue(4L))
            }
          },
          {
            "select table1.a, table2.a from table1, table2",
            Catalog.fromMap(
                Map.of(
                    "table1", Table.ofMap(typeSystem, "table1", Map.of("a", 0L, "b", 2L, "c", 3L)),
                    "table2", Table.ofMap(typeSystem, "table2", Map.of("a", 1L)))),
            new Row.Entry[] {
              Row.asEntry(Row.asKey("table1", "a", 0), Row.asValue(0L)),
              Row.asEntry(Row.asKey("table2", "a", 1), Row.asValue(1L))
            }
          },
          {
            "select table.a, table.b from table where table.a >= 1",
            Catalog.fromMap(
                Map.of(
                    "table",
                    Table.ofListOfMaps(
                        typeSystem,
                        "table",
                        List.of(
                            Map.of("a", 0L, "b", 10L),
                            Map.of("a", 1L, "b", 10L),
                            Map.of("a", 2L, "b", 10L))))),
            List.of(
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("table", "a", 0), Row.asValue(1L)),
                  Row.asEntry(Row.asKey("table", "b", 1), Row.asValue(10L))
                },
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("table", "a", 0), Row.asValue(2L)),
                  Row.asEntry(Row.asKey("table", "b", 1), Row.asValue(10L))
                })
          },
          {
            "select table1.a, table2.a from table1, table2 where table1.a != table2.a",
            Catalog.fromMap(
                Map.of(
                    "table1",
                        Table.ofListOfMaps(
                            typeSystem,
                            "table1",
                            List.of(Map.of("a", 1L, "b", 2L), Map.of("a", 3L, "b", 4L))),
                    "table2",
                        Table.ofListOfMaps(
                            typeSystem, "table2", List.of(Map.of("a", 1L), Map.of("a", 3L))))),
            List.of(
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("table1", "a", 0), Row.asValue(1L)),
                  Row.asEntry(Row.asKey("table2", "a", 0), Row.asValue(3L)),
                },
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("table1", "a", 0), Row.asValue(3L)),
                  Row.asEntry(Row.asKey("table2", "a", 0), Row.asValue(1L)),
                })
          },
          {
            "select event.a->'b' from event",
            Map.of("a", Map.of("b", 1)),
            new Row.Entry[] {Row.asEntry(Row.asKey("event", "col_1", 0), Row.asValue(1))}
          },
          {
            "select json_array_elements(a)->'b' from event",
            Map.of("a", List.of(Map.of("b", 1), Map.of("b", 2))),
            List.of(
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("event", "col_2", 0), Row.asValue(1)),
                },
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("event", "col_2", 0), Row.asValue(2)),
                })
          },
          {
            "select json_array_elements(a)->'b' v from event",
            Map.of("a", List.of(Map.of("b", 1), Map.of("b", 2))),
            List.of(
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("event", "v", 0), Row.asValue(1)),
                },
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("event", "v", 0), Row.asValue(2)),
                })
          },
          {
            "select json_array_elements(a)->'b' v1, json_array_elements(c) v2, d from event",
            Map.of("a", List.of(Map.of("b", 1), Map.of("b", 2)), "c", List.of(1, 2, 3), "d", "d"),
            List.of(
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("event", "v1", 0), Row.asValue(1)),
                  Row.asEntry(Row.asKey("rel_2", "v2", 0), Row.asValue(1)),
                  Row.asEntry(Row.asKey("event", "d", 0), Row.asValue("d")),
                },
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("event", "v1", 0), Row.asValue(2)),
                  Row.asEntry(Row.asKey("rel_2", "v2", 0), Row.asValue(2)),
                  Row.asEntry(Row.asKey("event", "d", 0), Row.asValue("d")),
                },
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("event", "v1", 0), Row.asValue(null)),
                  Row.asEntry(Row.asKey("rel_2", "v2", 0), Row.asValue(3)),
                  Row.asEntry(Row.asKey("event", "d", 0), Row.asValue("d")),
                })
          }, // select json_array_elements(a)->'b' v from event
          {
            "SELECT json_array_elements(articles)->'text' FROM event",
            Map.of("articles", List.of(Map.of("text", "abc"), Map.of("text", "edf"))),
            List.of(
                new Row.Entry[] {Row.asEntry(Row.asKey("event", "col_2", 0), Row.asValue("abc"))},
                new Row.Entry[] {Row.asEntry(Row.asKey("event", "col_2", 0), Row.asValue("edf"))})
          },
          {
            "SELECT json_array_elements(articles) FROM event",
            Map.of("articles", List.of(Map.of("text", "abc"), Map.of("text", "edf"))),
            List.of(
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("rel_1", "col_1", 0), Row.asValue(Map.of("text", "abc")))
                },
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("rel_1", "col_1", 0), Row.asValue(Map.of("text", "edf")))
                })
          },
                    {
                      "SELECT b.b from event, json_array_elements(event.b) b group by b.b",
                      Catalog.fromMap(
                          Map.of(
                              "event",
                                  Table.ofListOfMaps(
                                      typeSystem,
                                      "event",
                                      List.of(Map.of("a", 1L, "b", List.of(1,2,3)),
                                              Map.of("a", 1L, "b", List.of(1)),
                                              Map.of("a", 2L, "b", List.of(1,2)))))),
                      List.of(
                          new Row.Entry[] {
                            Row.asEntry(Row.asKey("b", "b", 0), Row.asValue(1L)),
                          },
                          new Row.Entry[] {
                            Row.asEntry(Row.asKey("b", "b", 0), Row.asValue(2L)),
                          },
                          new Row.Entry[] {
                            Row.asEntry(Row.asKey("b", "b", 0), Row.asValue(3L)),
                          }
                          )
                    },
                {
                        "SELECT b.b, string_agg(event.a, ',') from event, json_array_elements(event.b) b group by b.b",
                        Catalog.fromMap(
                                Map.of(
                                        "event",
                                        Table.ofListOfMaps(
                                                typeSystem,
                                                "event",
                                                List.of(Map.of("a", 1L, "b", List.of(1,2,3)),
                                                        Map.of("a", 1L, "b", List.of(1)),
                                                        Map.of("a", 2L, "b", List.of(1,2)))))),
                        List.of(
                                new Row.Entry[] {
                                        Row.asEntry(Row.asKey("b", "b", 0), Row.asValue(1L)),
                                        Row.asEntry(Row.asKey("event", "col_1", 0), Row.asValue("1,1,2")),

                                },
                                new Row.Entry[] {
                                        Row.asEntry(Row.asKey("b", "b", 0), Row.asValue(2L)),
                                        Row.asEntry(Row.asKey("event", "col_1", 0), Row.asValue("1,2")),
                                },
                                new Row.Entry[] {
                                        Row.asEntry(Row.asKey("b", "b", 0), Row.asValue(3L)),
                                        Row.asEntry(Row.asKey("event", "col_1", 0), Row.asValue("1")),
                                }
                        )
                },
                {
                        "SELECT a, count(*) as b from event group by a",
                        Catalog.fromMap(
                                Map.of(
                                        "event",
                                        Table.ofListOfMaps(
                                                typeSystem,
                                                "event",
                                                List.of(Map.of("a", 1L, "b", List.of(1,2,3)),
                                                        Map.of("a", 1L, "b", List.of(1)),
                                                        Map.of("a", 2L, "b", List.of(1,2)))))),
                        List.of(
                                new Row.Entry[] {
                                        Row.asEntry(Row.asKey("event", "a", 0), Row.asValue(1L)),
                                        Row.asEntry(Row.asKey("event", "b", 0), Row.asValue(2L)),
                                },
                                new Row.Entry[] {
                                        Row.asEntry(Row.asKey("event", "a", 0), Row.asValue(2L)),
                                        Row.asEntry(Row.asKey("event", "b", 0), Row.asValue(1L)),
                                }
                        )
                },
                          {
            "select table.a, table.b from table limit 2",
            Catalog.fromMap(
                Map.of(
                    "table",
                    Table.ofListOfMaps(
                        typeSystem,
                        "table",
                        List.of(
                            Map.of("a", 0L, "b", 10L),
                            Map.of("a", 1L, "b", 10L),
                            Map.of("a", 2L, "b", 10L))))),
            List.of(
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("table", "a", 0), Row.asValue(0L)),
                  Row.asEntry(Row.asKey("table", "b", 1), Row.asValue(10L))
                },
                new Row.Entry[] {
                  Row.asEntry(Row.asKey("table", "a", 0), Row.asValue(1L)),
                  Row.asEntry(Row.asKey("table", "b", 1), Row.asValue(10L))
                })
          },
          {
            "SELECT * FROM event",
            Map.of("a", 0L, "b", 10L),
            asList(
                    new Row.Entry[] {
                            Row.asEntry(Row.asKey("event", "a", 0), Row.asValue(0L)),
                            Row.asEntry(Row.asKey("event", "b", 0), Row.asValue(10L)),

                    }
            ),
          },
          {
            "SELECT count(nums) as cnt FROM event, json_array_elements(b) as nums group by 1",
            Map.of("a", 0L, "b", new int[]{1,2,3}),
            asList(
                    new Row.Entry[] {
                            Row.asEntry(Row.asKey("event", "cnt", 0), Row.asValue(3L)),
                    }
            ),
          },
                {
                        "SELECT count(*) as cnt FROM event, json_array_elements(b)",
                        Map.of("a", 0L, "b", new int[]{1,2,3}),
                        asList(
                                new Row.Entry[] {
                                        Row.asEntry(Row.asKey("event", "cnt", 0), Row.asValue(3L)),
                                }
                        ),
                },
                {
                        "SELECT count(els) as cnt FROM event, json_array_elements(b) as els",
                        Map.of("a", 0L, "b", new int[]{1,2,3}),
                        asList(
                                new Row.Entry[] {
                                        Row.asEntry(Row.asKey("event", "cnt", 0), Row.asValue(3L)),
                                }
                        ),
                },
                {
                        "SELECT concat(els, a) as cnt FROM event, json_array_elements(b) as els",
                        Map.of("a", 0L, "b", new int[]{1,2,3}),
                        List.of(
                                new Row.Entry[] {
                                        Row.asEntry(Row.asKey("event", "cnt", 0), Row.asValue("10")),
                                },
                                new Row.Entry[] {
                                        Row.asEntry(Row.asKey("event", "cnt", 0), Row.asValue("20")),
                                },
                                new Row.Entry[] {
                                        Row.asEntry(Row.asKey("event", "cnt", 0), Row.asValue("30")),
                                }

                        ),
                },
                {
                        "SELECT json_array_length(b) as cnt FROM event",
                        Map.of("a", 0L, "b", new int[]{1,2,3}),
                        asList(
                                new Row.Entry[] {
                                        Row.asEntry(Row.asKey("event", "cnt", 0), Row.asValue(3L)),
                                }
                        ),
                },

        };

    var interpreter = Interpreter.astInterpreter(typeSystem);
    var relInterpreter = new RelationalInterpreter<>(typeSystem, interpreter);

    for (int i = 0; i < testData.length; i++) {
      System.out.println("Testing item [" + i + "]");
      var testItem = testData[i];
      var sql = testItem[0].toString();
      Catalog catalog;

      if (testItem[1] instanceof Catalog) {
        catalog = (Catalog) testItem[1];
      } else {
        var event = (Map<String, Object>) testItem[1];
        var source = Table.ofMap(typeSystem, "event", event);
        catalog = Catalog.fromMap(Map.of("event", source));
      }

      var expected = testItem[2];

      QueryAST.Query query = QueryASTParser.astParser().parseSelectStatement(sql);

      var translation = QueryTranslator.translate(query);
      Iterator<Row> it = relInterpreter.query(translation, catalog);
      var result = Streams.asStream(it).toList();

      if (expected instanceof List<?> ls) {
          Assertions.assertEquals(ls.size(), result.size());
        for (int a = 0; a < ls.size(); a++) {
          var expectedEntries = ls.get(a);
          var resultEntries = result.get(a).getEntries();
          assertEquals((Row.Entry[]) expectedEntries, resultEntries);
        }
      } else {
        Assertions.assertEquals(
            result.size(),
            1,
            "expecting a single result but more were returned: "
                + Arrays.toString(result.toArray(new Row[0])));
        assertEquals((Row.Entry[]) expected, result.get(0).getEntries());
      }
    }
  }

  private void assertEquals(Row.Entry[] expected, Row.Entry[] entries) {

    Assertions.assertEquals(expected.length, entries.length);

    for (Row.Entry value : expected) {
      var entry = Row.findEntry(value.key(), entries);
      Assertions.assertNotNull(entry, "cannot find entry for " + value.key());
      Assertions.assertEquals(
          value.value(),
          entry.value(),
          "values for "
              + entry.key()
              + " do not match expected[ "
              + value.value()
              + " ] != given [ "
              + entry.value()
              + " ]");
    }
  }

  private List<Row.Entry[]> asList(Row.Entry[] entry) {
    var l = new ArrayList<Row.Entry[]>();
    l.add(entry);
    return l;
  }
}
