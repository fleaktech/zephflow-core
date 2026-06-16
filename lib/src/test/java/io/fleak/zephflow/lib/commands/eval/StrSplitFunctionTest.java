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

import io.fleak.zephflow.api.structure.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StrSplitFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testStrSplitWithSpecialRegexCharacters() {
    String[] testCases = {
      "str_split(\"a.b.c\", \".\")",
      "str_split(\"a|b|c\", \"|\")",
      "str_split(\"a*b*c\", \"*\")",
      "str_split(\"a+b+c\", \"+\")",
      "str_split(\"a[b]c\", \"[\")"
    };

    FleakData testData = new StringPrimitiveFleakData("test");

    for (String testCase : testCases) {
      try {
        FleakData result = evaluateExpression(testCase, testData);
        assertNotNull(result);
        assertInstanceOf(ArrayFleakData.class, result);
      } catch (Exception e) {
        fail("str_split with regex characters failed: " + testCase + ". Error: " + e.getMessage());
      }
    }
  }

  @Test
  public void testStrSplitEmptyString() {
    FleakData testData = new StringPrimitiveFleakData("test");

    testFunctionExecution(testData, "str_split(\"\", \",\")", List.of());
  }

  @Test
  public void testStrSplitNullHandling() {
    FleakData testData = FleakData.wrap(Map.of("text", "a,b,c"));

    testFunctionExecution(testData, "str_split(null, \",\")", null);
    testFunctionExecution(testData, "str_split($.nonexistent, \",\")", null);
    testFunctionExecution(testData, "str_split(\"a,b,c\", null)", List.of("a,b,c"));
    testFunctionExecution(testData, "str_split(null, null)", null);
    testFunctionExecution(testData, "str_split($.text, \",\")", List.of("a", "b", "c"));
    testFunctionExecution(
        testData,
        """
        str_split("a,b,,", ",")
        """,
        List.of("a", "b", "", ""));
    testFunctionExecution(
        testData,
        """
        str_split(",a,b", ",")
        """,
        List.of("", "a", "b"));
    testFunctionExecution(
        testData,
        """
        str_split(",,", ",")
        """,
        List.of("", "", ""));
    testFunctionExecution(
        testData,
        """
        str_split("a\\"b\\"c", "\\"")
        """,
        List.of("a", "b", "c"));
  }
}
