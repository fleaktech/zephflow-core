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
package io.fleak.zephflow.lib.commands.eval;

import io.fleak.zephflow.api.structure.FleakData;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LargeIntegerEvalTest extends FeelFunctionTestBase {

  private static final FleakData EVENT = FleakData.wrap(Map.of("id", 9007199254740993L));

  @Test
  void arithmeticOnLongsStaysExact() {
    testFunctionExecution(EVENT, "$.id", 9007199254740993L);
    testFunctionExecution(EVENT, "$.id + 1", 9007199254740994L);
    testFunctionExecution(EVENT, "$.id - 2", 9007199254740991L);
    testFunctionExecution(EVENT, "$.id * 1", 9007199254740993L);
    testFunctionExecution(EVENT, "$.id % 10", 3L);
    testFunctionExecution(EVENT, "-$.id", -9007199254740993L);
  }

  @Test
  void comparisonOnLongsStaysExact() {
    testFunctionExecution(EVENT, "$.id > 9007199254740992", true);
    testFunctionExecution(EVENT, "$.id == 9007199254740992", false);
    testFunctionExecution(EVENT, "$.id == 9007199254740993", true);
  }

  @Test
  void longLiteralsAndParsedLongsStayExact() {
    testFunctionExecution(EVENT, "9007199254740993", 9007199254740993L);
    testFunctionExecution(EVENT, "parse_int(\"9007199254740993\")", 9007199254740993L);
  }

  @Test
  void ceilFloorAndNumCompareAnyStayExact() {
    testFunctionExecution(EVENT, "ceil($.id)", 9007199254740993L);
    testFunctionExecution(EVENT, "floor($.id)", 9007199254740993L);
    testFunctionExecution(EVENT, "num_compare_any($.id, \"gt\", 9007199254740992)", true);
    testFunctionExecution(EVENT, "num_compare_any($.id, \"lte\", 9007199254740992)", false);
    testFunctionExecution(
        EVENT, "num_compare_any(\"9007199254740993\", \"gt\", 9007199254740992)", true);
  }

  @Test
  void negatingLongMinValueFallsBackToDouble() {
    testFunctionExecution(
        FleakData.wrap(Map.of("v", Long.MIN_VALUE)), "-$.v", 9.223372036854775808E18);
  }

  @Test
  void longOverflowPromotesToDouble() {
    testFunctionExecution(EVENT, "9223372036854775807 + 1", 9.223372036854775808E18);
    testFunctionExecution(EVENT, "(9223372036854775807 * 4) / 4", 9.223372036854775807E18);
  }

  @Test
  void mixedLongAndDoubleStillPromotesToDouble() {
    testFunctionExecution(EVENT, "$.id + 0.5", 9.007199254740992E15);
  }
}
