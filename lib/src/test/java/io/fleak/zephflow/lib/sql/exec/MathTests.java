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

import static io.fleak.zephflow.api.structure.NumberPrimitiveFleakData.DOUBLE_COMPARE_DELTA;

import io.fleak.zephflow.lib.sql.SQLInterpreter;
import io.fleak.zephflow.lib.sql.TestSQLUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MathTests {

  @Test
  public void testAbs() {
    var rows = runSQL("select abs(-5) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(5.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testCeil() {
    var rows = runSQL("select ceil(4.2) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(5.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testFloor() {
    var rows = runSQL("select floor(4.8) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(4.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testRound() {
    var rows = runSQL("select round(4.567, 2) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(4.57, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testSqrt() {
    var rows = runSQL("select sqrt(16) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(4.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testPower() {
    var rows = runSQL("select power(2, 3) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(8.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testMod() {
    var rows = runSQL("select mod(10, 3) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(1.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testRandom() {
    var rows = runSQL("select random() as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertTrue((Double) rows.get(0).asMap().get("col1") >= 0.0);
    Assertions.assertTrue((Double) rows.get(0).asMap().get("col1") <= 1.0);
  }

  @Test
  public void testTrunc() {
    var rows = runSQL("select trunc(4.567, 2) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(4.56, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testSign() {
    var rows = runSQL("select sign(-5) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(-1.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testGcd() {
    var rows = runSQL("select gcd(12, 15) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(3, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testLcm() {
    var rows = runSQL("select lcm(12, 15) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(60, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testExp() {
    var rows = runSQL("select exp(1) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertTrue(
        Math.abs(Math.E - ((Double) rows.get(0).asMap().get("col1"))) < DOUBLE_COMPARE_DELTA);
  }

  @Test
  public void testLog() {
    var rows = runSQL("select log(2.718281828459045) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(1.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testLn() {
    var rows = runSQL("select ln(2.718281828459045) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(1.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testLog10() {
    var rows = runSQL("select log10(100) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(2.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testLog2() {
    var rows = runSQL("select log2(8) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(3.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testDegrees() {
    var rows = runSQL("select degrees(pi()) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(180.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testRadians() {
    var rows = runSQL("select radians(180) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(Math.PI, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testPi() {
    var rows = runSQL("select pi() as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(Math.PI, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testSin() {
    var rows = runSQL("select sin(pi() / 2) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(1.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testCos() {
    var rows = runSQL("select cos(0) as col1").toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(1.0, rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testTan() {
    var rows = runSQL("select tan(pi() / 4) as col1").toList();
    Assertions.assertEquals(1, rows.size());
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
