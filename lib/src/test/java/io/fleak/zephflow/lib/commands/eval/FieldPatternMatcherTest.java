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

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.regex.PatternSyntaxException;
import org.junit.jupiter.api.Test;

class FieldPatternMatcherTest extends FeelFunctionTestBase {

  @Test
  void preservesUnpairedUtf16InCompiledPatternAndFieldLiterals() throws Exception {
    String high = String.valueOf((char) 0xd800);
    String otherHigh = String.valueOf((char) 0xd801);
    RecordFleakData record =
        event(
            "{"
                + FieldPopulationSelector.quote(high)
                + ":"
                + FieldPopulationSelector.quote(high)
                + "}");
    String predicate = expression(high, "[" + high + "]");
    testFunctionExecution(record, predicate, true);
    testFunctionExecution(record, "not " + predicate, false);
    testFunctionExecution(record, expression(high, "[" + otherHigh + "]"), false);
    assertEquals(1, filter(predicate, List.of(record)).size());
    assertEquals(0, filter("not " + predicate, List.of(record)).size());
  }

  @Test
  void usesSubstringSearchAndHonorsAnchorsAndEmbeddedFlags() throws Exception {
    RecordFleakData input = event("{\"message\":\"prefix ERROR suffix\\nnext\"}");
    testFunctionExecution(input, expression("message", "ERROR"), true);
    testFunctionExecution(input, expression("message", "^ERROR$"), false);
    testFunctionExecution(input, expression("message", "(?mi)^NEXT$"), true);
    testFunctionExecution(input, expression("message", "(?s)ERROR.*next"), true);
    testFunctionExecution(input, expression("message", "/ERROR/"), false);
    testFunctionExecution(input, expression("message", " ERROR "), true);
  }

  @Test
  void separatesWholeRecordsIncludingArraysNullContainersAndNumbers() throws Exception {
    List<RecordFleakData> records =
        List.of(
            event("{\"id\":1,\"v\":[null,\"x200y\",200]}"),
            event("{\"id\":2,\"v\":200}"),
            event("{\"id\":3,\"v\":true}"),
            event("{\"id\":4,\"v\":null}"),
            event("{\"id\":5,\"v\":{}}"),
            event("{\"id\":6,\"v\":[]}"),
            event("{\"id\":7}"),
            event("{\"id\":8,\"v\":\"other\"}"));
    String predicate = expression("v", "200|true");
    assertEquals(List.of(1L, 2L, 3L), ids(filter(predicate, records)));
    assertEquals(List.of(4L, 5L, 6L, 7L, 8L), ids(filter("not " + predicate, records)));
    for (RecordFleakData record : records) {
      boolean matched =
          FieldPatternMatcher.evaluate(record, "v", FieldPatternMatcher.compile("200|true"))
              .matched();
      testFunctionExecution(record, predicate, matched);
      testFunctionExecution(record, "not " + predicate, !matched);
    }
  }

  @Test
  void returnsEveryGroupZeroMatchAtTheCorrectPhysicalLocation() throws Exception {
    List<FieldPatternMatcher.MatchSpan> spans = new ArrayList<>();
    RecordFleakData input =
        event("{\"a.b\":\"aba\",\"a\":[{\"b\":[\"aa\",\"none\"]}],\"other\":\"aaa\"}");
    var result =
        FieldPatternMatcher.evaluate(
            input, "a.b", FieldPatternMatcher.compile("(a)"), () -> {}, spans::add);
    assertEquals(new FieldPatternMatcher.Result(true, true), result);
    assertEquals(
        List.of(0, 2, 0, 1), spans.stream().map(FieldPatternMatcher.MatchSpan::from).toList());
    assertEquals(
        List.of(1, 3, 1, 2), spans.stream().map(FieldPatternMatcher.MatchSpan::to).toList());
    assertEquals(
        List.of(new FieldPopulationSelector.ObjectKey("a.b")), spans.getFirst().location());
    assertEquals(
        List.of(
            new FieldPopulationSelector.ObjectKey("a"),
            new FieldPopulationSelector.ArrayIndex(0),
            new FieldPopulationSelector.ObjectKey("b"),
            new FieldPopulationSelector.ArrayIndex(0)),
        spans.get(2).location());
  }

  @Test
  void preservesZeroLengthMatchesAndUtf16Offsets() throws Exception {
    List<FieldPatternMatcher.MatchSpan> spans = new ArrayList<>();
    FieldPatternMatcher.evaluate(
        event("{\"v\":\"😀a\"}"),
        "v",
        FieldPatternMatcher.compile("(?=a)|$"),
        () -> {},
        spans::add);
    assertEquals(List.of(2, 3), spans.stream().map(FieldPatternMatcher.MatchSpan::from).toList());
    assertTrue(spans.stream().allMatch(span -> span.from() == span.to()));
    testFunctionExecution(event("{\"v\":\"\"}"), expression("v", "^$"), true);
    testFunctionExecution(event("{}"), expression("v", "^$"), false);
    testFunctionExecution(event("{\"v\":null}"), expression("v", ".*"), false);
  }

  @Test
  void preservesLiteralFieldsAndPatternsAcrossFeelEncoding() throws Exception {
    String field = "q\"\\\n\u0000";
    String text = "prefix q\"\\ suffix";
    ObjectNode node = JsonUtils.OBJECT_MAPPER.createObjectNode().put(field, text);
    RecordFleakData input = JsonUtils.fromJsonPayload(node);
    String pattern = java.util.regex.Pattern.quote("q\"\\");
    testFunctionExecution(input, expression(field, pattern), true);
    assertEquals(1, filter(expression(field, pattern), List.of(input)).size());
    testFunctionExecution(event("{\"\":\"x\"}"), expression("", "x"), true);
  }

  @Test
  void reportsInvalidSyntaxAndArgumentsWithoutReturningUnmatched() throws Exception {
    PatternSyntaxException failure =
        assertThrows(PatternSyntaxException.class, () -> FieldPatternMatcher.compile("["));
    assertTrue(failure.getIndex() >= 0);
    RecordFleakData input = event("{\"v\":\"x\"}");
    for (String value :
        List.of(
            "field_regex_search($)",
            "field_regex_search($, 1, \"x\")",
            "field_regex_search($, \"v\", null)",
            "field_regex_search(null, \"v\", \"x\")",
            expression("v", "["),
            "not " + expression("v", "["))) {
      assertThrows(Exception.class, () -> evaluateExpression(value, input), value);
    }
  }

  @Test
  void budgetIsSharedAcrossFindsAndNeverHiddenByAnEarlierMatchOrNegation() throws Exception {
    RecordFleakData input =
        (RecordFleakData)
            FleakData.wrap(
                Map.of(
                    "v", List.of("a", "a".repeat((int) FieldPatternMatcher.CHARACTER_BUDGET + 1))));
    var error =
        assertThrows(
            IllegalArgumentException.class,
            () -> FieldPatternMatcher.evaluate(input, "v", FieldPatternMatcher.compile("a")));
    assertTrue(error.getMessage().contains(FieldPatternMatcher.EVALUATION_LIMIT));
    assertThrows(Exception.class, () -> evaluateExpression("not " + expression("v", "a"), input));
    for (String predicate : List.of(expression("v", "a"), "not " + expression("v", "a"))) {
      var result = executeFilter(predicate, List.of(input));
      assertTrue(result.getOutput().isEmpty());
      assertEquals(1, result.getFailureEvents().size());
      assertSame(input, result.getFailureEvents().getFirst().inputEvent());
      assertTrue(
          result
              .getFailureEvents()
              .getFirst()
              .errorMessage()
              .contains(FieldPatternMatcher.EVALUATION_LIMIT));
    }
  }

  @Test
  void subsequencesShareTheOriginalCharacterBudget() {
    var budget = new FieldPatternMatcher.Budget(2, () -> {});
    var input = new FieldPatternMatcher.BudgetedText("abc", 0, 3, budget);
    assertEquals('a', input.charAt(0));
    CharSequence child = input.subSequence(1, 3).subSequence(0, 1);
    assertEquals('b', child.charAt(0));
    assertTrue(
        assertThrows(IllegalArgumentException.class, () -> child.charAt(0))
            .getMessage()
            .contains(FieldPatternMatcher.EVALUATION_LIMIT));
  }

  @Test
  void checksCancellationBeforeZeroWidthSearchAndCharacterReads() throws Exception {
    RecordFleakData input = event("{\"v\":\"abc\"}");
    assertThrows(
        CancellationException.class,
        () ->
            FieldPatternMatcher.evaluate(
                input,
                "v",
                FieldPatternMatcher.compile("^"),
                () -> {
                  throw new CancellationException("discarded");
                },
                null));
    Thread.currentThread().interrupt();
    try {
      assertThrows(
          CancellationException.class,
          () -> FieldPatternMatcher.evaluate(input, "v", java.util.regex.Pattern.compile("a")));
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void cacheKeepsExpressionAndCompiledPatternTogetherAcrossConcurrentCalls() throws Exception {
    FieldRegexSearchFunction function = new FieldRegexSearchFunction();
    RecordFleakData input = event("{\"v\":\"alpha\"}");
    try (var executor = Executors.newFixedThreadPool(4)) {
      List<java.util.concurrent.Future<Boolean>> results = new ArrayList<>();
      for (int index = 0; index < 200; index++) {
        boolean expected = index % 2 == 0;
        results.add(
            executor.submit(
                () -> {
                  var output =
                      function.evaluateCompiledEager(
                          null,
                          List.of(
                              input,
                              new StringPrimitiveFleakData("v"),
                              new StringPrimitiveFleakData(expected ? "alpha" : "beta")),
                          null);
                  return expected == ((BooleanPrimitiveFleakData) output).isTrueValue();
                }));
      }
      for (var result : results) assertTrue(result.get());
    }
  }

  @Test
  void cancelsWhileFindReadsCharactersAndBetweenZeroWidthMatches() throws Exception {
    var reads = new java.util.concurrent.atomic.AtomicInteger();
    RecordFleakData input = (RecordFleakData) FleakData.wrap(Map.of("v", "a".repeat(1000)));
    assertThrows(
        CancellationException.class,
        () ->
            FieldPatternMatcher.evaluate(
                input,
                "v",
                FieldPatternMatcher.compile("z"),
                () -> {
                  if (reads.incrementAndGet() == 10)
                    throw new CancellationException("cancelled in find");
                },
                null));
    assertEquals(10, reads.get());
    var matches = new java.util.concurrent.atomic.AtomicInteger();
    assertThrows(
        CancellationException.class,
        () ->
            FieldPatternMatcher.evaluate(
                input,
                "v",
                FieldPatternMatcher.compile("(?=)"),
                () -> {
                  if (matches.get() > 0) throw new CancellationException("cancelled between finds");
                },
                span -> matches.incrementAndGet()));
    assertEquals(1, matches.get());
  }

  @Test
  void doesNotMislabelErrorsThrownByTheSpanConsumer() throws Exception {
    RecordFleakData input = event("{\"v\":\"a\"}");
    StackOverflowError expected = new StackOverflowError("consumer failed");
    assertSame(
        expected,
        assertThrows(
            StackOverflowError.class,
            () ->
                FieldPatternMatcher.evaluate(
                    input,
                    "v",
                    FieldPatternMatcher.compile("a"),
                    () -> {},
                    span -> {
                      throw expected;
                    })));
  }

  @Test
  void honorsBackreferencesLookaroundsAndExactCombiningSequences() throws Exception {
    var repeated = (RecordFleakData) FleakData.wrap(Map.of("v", "prefix alpha alpha suffix"));
    testFunctionExecution(repeated, expression("v", "(?<=prefix )(\\w+) \\1(?= suffix)"), true);
    testFunctionExecution(repeated, expression("v", "(?<=prefix )(\\w+) \\1$"), false);
    String combining = "e" + (char) 0x0301;
    var record = (RecordFleakData) FleakData.wrap(Map.of("v", combining + " é"));
    List<FieldPatternMatcher.MatchSpan> spans = new ArrayList<>();
    FieldPatternMatcher.evaluate(
        record, "v", FieldPatternMatcher.compile(combining), () -> {}, spans::add);
    assertEquals(1, spans.size());
    assertEquals(0, spans.getFirst().from());
    assertEquals(2, spans.getFirst().to());
    testFunctionExecution(
        (RecordFleakData) FleakData.wrap(Map.of("v", combining)), expression("v", "é"), false);
  }

  @Test
  void reportsActualMatcherStackOverflowAsAnEvaluationFailure() throws Exception {
    var compiled = FieldPatternMatcher.compile("(a|aa)+$");
    var record = (RecordFleakData) FleakData.wrap(Map.of("v", "a".repeat(20000) + "!"));
    var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
    Thread thread =
        new Thread(
            null,
            () -> {
              try {
                FieldPatternMatcher.evaluate(record, "v", compiled);
              } catch (Throwable error) {
                failure.set(error);
              }
            },
            "pattern-stack-regression",
            256 * 1024);
    thread.start();
    thread.join(10000);
    if (thread.isAlive()) {
      thread.interrupt();
      thread.join(10000);
    }
    assertFalse(thread.isAlive());
    assertInstanceOf(IllegalArgumentException.class, failure.get());
    assertTrue(
        failure.get().getMessage().startsWith(FieldPatternMatcher.STACK_LIMIT),
        String.valueOf(failure.get()));
  }

  private static String expression(String field, String pattern) {
    return "field_regex_search($, "
        + FieldPopulationSelector.quote(field)
        + ", "
        + FieldPopulationSelector.quote(pattern)
        + ")";
  }

  private static RecordFleakData event(String json) throws Exception {
    return JsonUtils.fromJsonPayload((ObjectNode) JsonUtils.OBJECT_MAPPER.readTree(json));
  }

  private static List<Long> ids(List<RecordFleakData> records) {
    return records.stream()
        .map(record -> ((Number) record.getPayload().get("id").unwrap()).longValue())
        .toList();
  }

  private static List<RecordFleakData> filter(String expression, List<RecordFleakData> input)
      throws Exception {
    var result = executeFilter(expression, input);
    assertTrue(result.getFailureEvents().isEmpty(), result.getFailureEvents().toString());
    return result.getOutput();
  }

  private static ScalarCommand.ProcessResult executeFilter(
      String expression, List<RecordFleakData> input) throws Exception {
    ScalarCommand filter =
        (ScalarCommand)
            OperatorCommandRegistry.OPERATOR_COMMANDS
                .get("filter")
                .createCommand("pattern", JOB_CONTEXT);
    Map<String, Object> config =
        JsonUtils.fromJsonString(
            JsonUtils.toJsonString(Map.of("expression", expression)),
            new com.fasterxml.jackson.core.type.TypeReference<>() {});
    filter.parseAndValidateArg(config);
    filter.initialize(new MetricClientProvider.NoopMetricClientProvider());
    try {
      return filter.process(input, "pattern", filter.getExecutionContext());
    } finally {
      filter.terminate();
    }
  }
}
