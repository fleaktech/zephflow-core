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

import io.fleak.zephflow.lib.sql.SQLInterpreter;
import io.fleak.zephflow.lib.sql.TestSQLUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStringFunctions {

  @Test
  public void testLeft() {
    var rows = runSQL("select left('abcdef', 3) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("abc", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testRight() {
    var rows = runSQL("select right('abcdef', 3) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("def", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testReverse() {
    var rows = runSQL("select reverse('abcdef') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("fedcba", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testConcatWs() {
    var rows = runSQL("select concat_ws('-', 'abc', 'def', 'ghi') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("abc-def-ghi", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testCharLength() {
    var rows = runSQL("select char_length('abcdef') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(6, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testToHex() {
    var rows = runSQL("select to_hex(255) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("ff", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testBTrim() {
    var rows = runSQL("select btrim('xyxtrimxyx', 'xy') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("trim", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testPosition() {
    var rows = runSQL("select position('needle', 'haystack') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testMd5() {
    var rows = runSQL("select md5('abc') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("900150983cd24fb0d6963f7d28e17f72", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testRegexpMatches() {
    var rows = runSQL("select regexp_matches('foobarbequebaz', 'ba[rz]') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(List.of("bar", "baz"), rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testFormat() {
    var rows = runSQL("select format('Hello, %s!', 'world') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("Hello, world!", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testStringAgg() {
    var rows = runSQL("select string_agg(name, ', ') as col1 from records").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("abc, edf, ghi", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testReplace() {
    var rows = runSQL("select replace('abcdefabcdef', 'cd', 'XX') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("abXXefabXXef", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testRTrim() {
    var rows = runSQL("select rtrim('test   ') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("test", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testLTrim() {
    var rows = runSQL("select ltrim('   test') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("test", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testSubstring() {
    var rows = runSQL("select substring('abcdef', 2 , 3) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("bcd", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testTrim() {
    var rows = runSQL("select trim('   test   ') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("test", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testUpper() {
    var rows = runSQL("select upper('abc') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("ABC", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testLower() {
    var rows = runSQL("select lower('ABC') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("abc", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testRegexpReplace() {
    var rows = runSQL("select regexp_replace('abc def ghi', '\\s+', '-') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("abc-def-ghi", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testLength() {
    var rows = runSQL("select length('abcdef') as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(6, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testSplitPart() {
    var rows = runSQL("select split_part('abc,def,ghi', ',', 2) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("def", rows.get(0).asMap().get("col1"));
  }

  private static Stream<Row> runSQL(String sql) {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    return TestSQLUtils.runSQL(
        Catalog.fromMap(
            Map.of(
                "records",
                Table.ofListOfMaps(
                    typeSystem,
                    "records",
                    List.of(
                        Map.of("name", "abc", "id", 1),
                        Map.of("name", "edf", "id", 2),
                        Map.of("name", "ghi", "id", 3))),
                "ids",
                Table.ofListOfMaps(typeSystem, "ids", List.of(Map.of("id", 1), Map.of("id", 2))))),
        sql);
  }
}
