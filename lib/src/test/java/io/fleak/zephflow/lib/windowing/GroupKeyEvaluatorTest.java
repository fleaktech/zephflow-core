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
package io.fleak.zephflow.lib.windowing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.windowing.GroupKeyEvaluator.GroupKey;
import io.fleak.zephflow.lib.windowing.GroupKeyEvaluator.Kind;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GroupKeyEvaluatorTest {

  private static RecordFleakData rec(Map<String, Object> m) {
    return (RecordFleakData) FleakData.wrap(m);
  }

  private static GroupKey eval(String expr, Map<String, Object> event) {
    return GroupKeyEvaluator.compile(expr).evaluate(rec(event));
  }

  @Test
  void present_stringNumberBool() {
    assertEquals(GroupKey.present("h1"), eval("$.host", Map.of("host", "h1")));
    // integral number formats without a trailing ".0"
    assertEquals(GroupKey.present("500"), eval("$.code", Map.of("code", 500)));
    assertEquals(GroupKey.present("true"), eval("$.flag", Map.of("flag", true)));
  }

  @Test
  void missing_nullOrEmpty() {
    assertEquals(Kind.MISSING, eval("$.nope", Map.of("host", "h1")).kind());
    assertEquals(Kind.MISSING, eval("$.empty", Map.of("empty", "")).kind());
  }

  @Test
  void nonScalar_arrayOrMap() {
    assertEquals(Kind.NON_SCALAR, eval("$.arr", Map.of("arr", List.of(1, 2))).kind());
    assertEquals(Kind.NON_SCALAR, eval("$.obj", Map.of("obj", Map.of("a", 1))).kind());
  }

  @Test
  void error_whenEvaluationThrows() {
    // parse_int on a non-numeric string throws -> caught and classified as ERROR
    assertEquals(Kind.ERROR, eval("parse_int($.host)", Map.of("host", "not-a-number")).kind());
  }

  @Test
  void compile_throwsOnMalformedExpression() {
    assertThrows(Exception.class, () -> GroupKeyEvaluator.compile("* 3"));
  }
}
