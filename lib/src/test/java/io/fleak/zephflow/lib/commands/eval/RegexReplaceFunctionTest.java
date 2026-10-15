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
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import org.junit.jupiter.api.Test;

// FEEL string literals take a doubled backslash for regex escapes ("\\s" in FEEL), so each one is
// written "\\\\s" in the Java source below.
class RegexReplaceFunctionTest extends FeelFunctionTestBase {

  private static final FleakData ANY = new StringPrimitiveFleakData("test");

  private static FleakData ev(String json) {
    return JsonUtils.loadFleakDataFromJsonString(json);
  }

  @Test
  public void replacesEveryMatch() {
    testFunctionExecution(ANY, "regex_replace(\"a  b \t\tc\", \"\\\\s+\", \" \")", "a b c");
    testFunctionExecution(ANY, "regex_replace(\"a1b22c333\", \"\\\\d+\", \"#\")", "a#b#c#");
  }

  @Test
  public void noMatchReturnsInputUnchanged() {
    testFunctionExecution(ANY, "regex_replace(\"hello\", \"\\\\d+\", \"#\")", "hello");
  }

  @Test
  public void emptyReplacementDeletesMatches() {
    testFunctionExecution(ANY, "regex_replace(\"a-b-c\", \"-\", \"\")", "abc");
  }

  @Test
  public void captureGroupBackreferences() {
    testFunctionExecution(
        ANY,
        "regex_replace(\"2024-03-15\", \"(\\\\d+)-(\\\\d+)-(\\\\d+)\", \"$3/$2/$1\")",
        "15/03/2024");
    testFunctionExecution(
        ANY,
        "regex_replace(\"john smith\", \"(?<first>\\\\w+) (?<last>\\\\w+)\", \"${last}, ${first}\")",
        "smith, john");
  }

  @Test
  public void rewritesWindowsXmlDataElementsToKeyValue() {
    testFunctionExecution(
        ev("{\"raw\":\"<Data Name='SubjectUserName'>bob</Data><Data Name='LogonType'>3</Data>\"}"),
        "regex_replace($.raw, \"<Data Name='(\\\\w+)'>([^<]*)</Data>\", \"$1=$2 \")",
        "SubjectUserName=bob LogonType=3 ");
  }

  @Test
  public void inlineFlagsControlMatchingMode() {
    // (?s): . also matches newlines, so a multi-line block is stripped in one match
    testFunctionExecution(
        ev("{\"raw\":\"<Event><System>\\n<EventID>4624</EventID>\\n</System><Data/></Event>\"}"),
        "regex_replace($.raw, \"(?s)<System>.*?</System>\", \"\")",
        "<Event><Data/></Event>");
    // (?i): case-insensitive
    testFunctionExecution(
        ANY, "regex_replace(\"Error ERROR error\", \"(?i)error\", \"E\")", "E E E");
  }

  @Test
  public void escapedDollarIsLiteral() {
    testFunctionExecution(ANY, "regex_replace(\"price 5\", \"(\\\\d+)\", \"\\\\$$1\")", "price $5");
  }

  @Test
  public void nullInputReturnsNull() {
    testFunctionExecution(ANY, "regex_replace(null, \"a\", \"b\")", null);
    testFunctionExecution(ev("{\"g\":1}"), "regex_replace($.f, \"a\", \"b\")", null);
  }

  @Test
  public void rejectsNonStringArguments() {
    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("regex_replace(123, \"\\\\d\", \"x\")", ANY));
    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("regex_replace(\"abc\", null, \"x\")", ANY));
    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("regex_replace(\"abc\", \"a\", null)", ANY));
  }

  @Test
  public void rejectsInvalidPattern() {
    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("regex_replace(\"abc\", \"(unclosed\", \"x\")", ANY));
  }

  @Test
  public void emptyInputStaysEmpty() {
    testFunctionExecution(ANY, "regex_replace(\"\", \"a\", \"x\")", "");
  }

  @Test
  public void emptyMatchingPatternInsertsAtEveryPosition() {
    // a pattern that can match the empty string matches between every character, as in Java/JS
    testFunctionExecution(ANY, "regex_replace(\"abc\", \"x*\", \"-\")", "-a-b-c-");
  }

  @Test
  public void unmatchedOptionalGroupInsertsEmptyString() {
    testFunctionExecution(ANY, "regex_replace(\"b\", \"(a)?b\", \"[$1]\")", "[]");
  }

  @Test
  public void escapedBackslashIsLiteral() {
    testFunctionExecution(ANY, "regex_replace(\"a.b\", \"\\\\.\", \"\\\\\\\\\")", "a\\b");
  }

  @Test
  public void matchesUnicodeLetters() {
    testFunctionExecution(ANY, "regex_replace(\"café 日本\", \"\\\\p{L}+\", \"W\")", "W W");
  }

  @Test
  public void caretMatchesEachLineOnlyWithMultilineFlag() {
    FleakData twoLines = ev("{\"raw\":\"a\\nb\"}");
    testFunctionExecution(twoLines, "regex_replace($.raw, \"^\", \"> \")", "> a\nb");
    testFunctionExecution(twoLines, "regex_replace($.raw, \"(?m)^\", \"> \")", "> a\n> b");
  }

  @Test
  public void rejectsReferenceToMissingGroup() {
    assertInvalidReplacement("regex_replace(\"abc\", \"(a)\", \"$2\")");
  }

  @Test
  public void rejectsReferenceToMissingNamedGroup() {
    assertInvalidReplacement("regex_replace(\"abc\", \"(?<n>b)\", \"${foo}\")");
  }

  @Test
  public void rejectsTrailingLoneDollar() {
    assertInvalidReplacement("regex_replace(\"abc\", \"b\", \"x$\")");
  }

  private void assertInvalidReplacement(String expression) {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> evaluateExpression(expression, ANY));
    assertTrue(e.getMessage().startsWith("regex_replace: replacement"), e.getMessage());
  }

  @Test
  public void rejectsWrongArgumentCount() {
    IllegalArgumentException tooFew =
        assertThrows(
            IllegalArgumentException.class,
            () -> evaluateExpression("regex_replace(\"abc\", \"a\")", ANY));
    assertEquals("regex_replace expects 3 arguments but got 2", tooFew.getMessage());

    IllegalArgumentException tooMany =
        assertThrows(
            IllegalArgumentException.class,
            () -> evaluateExpression("regex_replace(\"abc\", \"a\", \"b\", \"c\")", ANY));
    assertEquals("regex_replace expects 3 arguments but got 4", tooMany.getMessage());
  }
}
