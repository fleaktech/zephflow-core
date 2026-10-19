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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.CompiledExpression;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionCompiler;
import io.fleak.zephflow.lib.commands.eval.compiled.nodes.ErrorNode;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Compile-time scope analysis: unbound variable references are rejected (FLE-2884). */
class ExpressionCompilerScopeTest {

  @Test
  void bareUndefinedName_isRejected() {
    assertEquals("variable 'dafew' is not defined", compileError("dafew"));
  }

  @Test
  void loopVarUsedOutsideItsBinder_isRejected() {
    assertEquals("variable 'x' is not defined", compileError("arr_foreach($.items, x, x) and x"));
  }

  @Test
  void binderArrayArgReferencingTheNotYetBoundName_isRejected() {
    // the first x (array arg) is unbound; the body x is bound and fine
    assertEquals("variable 'x' is not defined", compileError("arr_foreach(x, x, x)"));
  }

  @Test
  void undefinedVariableInsideBinderBody_isRejected() {
    // body references y, not the bound loop var x
    assertEquals("variable 'y' is not defined", compileError("arr_foreach($.items, x, y)"));
  }

  @Test
  void undefinedVariableInArrayIndex_isRejected() {
    // the index expression of a path step is a reference too
    assertEquals("variable 'k' is not defined", compileError("$.a[k]"));
  }

  @Test
  void undefinedVariableInCaseResult_isRejected() {
    assertEquals("variable 'y' is not defined", compileError("case($.x == 1 => y, _ => 0)"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "arr_foreach($.items, x, x + 1)",
        "arr_foreach($.items, item, item.id)",
        "arr_find($.items, elem, elem.ok)",
        "arr_filter($.items, e, e > 0)",
        "arr_to_dict($.items, idx, idx.k, idx.v)",
        // nested binder: inner body sees the outer binding
        "arr_foreach($.a, x, arr_foreach(x, y, x + y))",
        // a bound loop var used as an array index is fine
        "arr_filter($.items, i, $.data[i] > 0)",
        // field access into the event is never a variable reference
        "$",
        "$.a.b",
        "$.items[0]",
        "dict(k=$.a, j=$.b)",
        "case($.type == 'a' => 1, _ => 0)",
      })
  void boundAndFieldReferences_compile(String expression) {
    assertDoesNotThrow(() -> compile(expression));
  }

  @Test
  void lenientMode_swallowsUnboundVariableIntoErrorNode() {
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput("dafew", AntlrUtils.GrammarType.EVAL);
    CompiledExpression compiled = ExpressionCompiler.compile(parser.language(), null, true);
    assertInstanceOf(ErrorNode.class, compiled.root());
  }

  private static CompiledExpression compile(String expression) {
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    return ExpressionCompiler.compile(parser.language(), null);
  }

  private static String compileError(String expression) {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> compile(expression));
    return e.getMessage();
  }
}
