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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExpressionCacheTest {

  @Test
  void testCacheIsUsedForStringLiterals() {
    String expression = "dict(key='value', other=\"test\")";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(FleakData.wrap(Map.of()), null, cache);
    FleakData result = visitor.visit(ctx);

    Map<String, Object> expected = Map.of("key", "value", "other", "test");
    assertEquals(expected, result.unwrap());
  }

  @Test
  void testCacheWithFieldAccess() {
    String expression = "$.foo.bar";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    FleakData input = FleakData.wrap(Map.of("foo", Map.of("bar", "baz")));
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(input, null, cache);
    FleakData result = visitor.visit(ctx);

    assertEquals("baz", result.unwrap());
  }

  @Test
  void testCacheWithBinaryOperators() {
    String expression = "1 + 2 * 3 - 4";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(FleakData.wrap(Map.of()), null, cache);
    FleakData result = visitor.visit(ctx);

    assertEquals(3L, result.unwrap());
  }

  @Test
  void testCacheWithComplexExpression() {
    String expression =
        """
dict(
  version='TLSv1.3',
  status=case($.type == 'ok' => 'success', _ => 'failed'),
  items=array('a', 'b', 'c')
)
""";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    FleakData input = FleakData.wrap(Map.of("type", "ok"));
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(input, null, cache);
    FleakData result = visitor.visit(ctx);

    Map<String, Object> expected =
        Map.of("version", "TLSv1.3", "status", "success", "items", List.of("a", "b", "c"));
    assertEquals(expected, result.unwrap());
  }

  @Test
  void testCacheWithQuotedFieldAccess() {
    String expression = "$[\"special-field\"]";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    FleakData input = FleakData.wrap(Map.of("special-field", "value123"));
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(input, null, cache);
    FleakData result = visitor.visit(ctx);

    assertEquals("value123", result.unwrap());
  }

  @Test
  void testCacheReuseAcrossMultipleEvents() {
    String expression = "dict(upper_name=upper($.name), lower_name=lower($.name))";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    FleakData input1 = FleakData.wrap(Map.of("name", "Alice"));
    ExpressionValueVisitor visitor1 = ExpressionValueVisitor.createInstance(input1, null, cache);
    FleakData result1 = visitor1.visit(ctx);
    assertEquals(Map.of("upper_name", "ALICE", "lower_name", "alice"), result1.unwrap());

    FleakData input2 = FleakData.wrap(Map.of("name", "Bob"));
    ExpressionValueVisitor visitor2 = ExpressionValueVisitor.createInstance(input2, null, cache);
    FleakData result2 = visitor2.visit(ctx);
    assertEquals(Map.of("upper_name", "BOB", "lower_name", "bob"), result2.unwrap());

    FleakData input3 = FleakData.wrap(Map.of("name", "Charlie"));
    ExpressionValueVisitor visitor3 = ExpressionValueVisitor.createInstance(input3, null, cache);
    FleakData result3 = visitor3.visit(ctx);
    assertEquals(Map.of("upper_name", "CHARLIE", "lower_name", "charlie"), result3.unwrap());
  }

  @Test
  void testCacheWithUnaryOperator() {
    String expression = "-42";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(FleakData.wrap(Map.of()), null, cache);
    FleakData result = visitor.visit(ctx);

    assertEquals(-42L, result.unwrap());
  }

  @Test
  void testCacheWithComparisonOperators() {
    String expression = "1 < 2 and 3 >= 3 or 5 == 5";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(FleakData.wrap(Map.of()), null, cache);
    FleakData result = visitor.visit(ctx);

    assertEquals(true, result.unwrap());
  }

  @Test
  void testCacheStepsForDeepPathExpression() {
    String expression = "$.level1.level2.level3.level4";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    FleakData input =
        FleakData.wrap(Map.of("level1", Map.of("level2", Map.of("level3", Map.of("level4", 42)))));
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(input, null, cache);
    FleakData result = visitor.visit(ctx);

    assertEquals(42L, result.unwrap());
  }

  @Test
  void testCacheDictKvPairs() {
    String expression = "dict(a=1, b=2, c=3, d=4, e=5)";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(FleakData.wrap(Map.of()), null, cache);
    FleakData result = visitor.visit(ctx);

    Map<String, Object> expected = Map.of("a", 1L, "b", 2L, "c", 3L, "d", 4L, "e", 5L);
    assertEquals(expected, result.unwrap());
  }

  @Test
  void testCacheCaseWhenClauses() {
    String expression =
        "case($.val == 1 => 'one', $.val == 2 => 'two', $.val == 3 => 'three', _ => 'other')";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    FleakData input1 = FleakData.wrap(Map.of("val", 2));
    ExpressionValueVisitor visitor1 = ExpressionValueVisitor.createInstance(input1, null, cache);
    assertEquals("two", visitor1.visit(ctx).unwrap());

    FleakData input2 = FleakData.wrap(Map.of("val", 99));
    ExpressionValueVisitor visitor2 = ExpressionValueVisitor.createInstance(input2, null, cache);
    assertEquals("other", visitor2.visit(ctx).unwrap());
  }

  @Test
  void testCacheArgumentExpressions() {
    String expression = "array('a', 'b', 'c', 'd', 'e')";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext ctx = parser.language();

    ExpressionCache cache = ExpressionCache.build(ctx);

    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(FleakData.wrap(Map.of()), null, cache);
    FleakData result = visitor.visit(ctx);

    assertEquals(List.of("a", "b", "c", "d", "e"), result.unwrap());
  }
}
