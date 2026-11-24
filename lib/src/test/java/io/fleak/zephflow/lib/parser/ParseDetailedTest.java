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
package io.fleak.zephflow.lib.parser;

import static io.fleak.zephflow.lib.parser.CompiledRules.InputKind.*;
import static io.fleak.zephflow.lib.parser.CompiledRules.TargetOutcome.*;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.parser.extractions.JsonExtractionConfig;
import io.fleak.zephflow.lib.parser.extractions.KvPairExtractionConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class ParseDetailedTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void exactNestedAndLiteralTargetsPreserveSource(boolean remove) {
    var original =
        Map.of(
            "payload",
            Map.of("message", "host=inner", "sibling", "kept"),
            "payload.message",
            "host=literal",
            "host",
            "inner");
    var input = record(original);
    var nested = kv("payload.message", List.of("payload", "message"), remove);
    var result = nested.parseDetailed(input);
    assertEquals(nested.parse(input), result.outputRecord());
    assertEquals(EXTRACTED, result.targetOutcome());
    assertEquals(Set.of("host"), result.extractedKeys());
    assertEquals(List.of("payload", "message"), result.events().getFirst().targetFieldPath());
    assertEquals(remove, result.events().getFirst().removedTarget());
    assertEquals("inner", result.outputRecord().unwrap().get("host"));
    assertEquals(
        remove ? Map.of("sibling", "kept") : original.get("payload"),
        result.outputRecord().unwrap().get("payload"));
    assertEquals(original, input.unwrap());

    var literal = kv("payload.message", null, remove);
    var literalResult = literal.parseDetailed(input);
    assertEquals(literal.parse(input), literalResult.outputRecord());
    assertEquals(List.of("payload.message"), literalResult.events().getFirst().targetFieldPath());
    assertEquals("literal", literalResult.outputRecord().unwrap().get("host"));
    assertEquals(!remove, literalResult.outputRecord().getPayload().containsKey("payload.message"));
    assertEquals(original.get("payload"), literalResult.outputRecord().unwrap().get("payload"));
    assertEquals(original, input.unwrap());
  }

  @Test
  void removalRetainsEmptyParentsAndCopiesOnlyChangedAncestors() {
    var input = record(Map.of("payload", Map.of("", "host=inner"), "other", Map.of("x", 1)));
    var result = kv("payload[empty]", List.of("payload", ""), true).parseDetailed(input);
    assertEquals(Map.of(), result.outputRecord().unwrap().get("payload"));
    assertNotSame(
        input.getPayload().get("payload"), result.outputRecord().getPayload().get("payload"));
    assertSame(input.getPayload().get("other"), result.outputRecord().getPayload().get("other"));
    assertEquals(Map.of("", "host=inner"), input.unwrap().get("payload"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void noExtractionShowsActualOutputAndSourceRemoval(boolean remove) {
    var input = record(Map.of("message", "startup"));
    var rule = kv("message", null, remove);
    var result = rule.parseDetailed(input);
    assertEquals(rule.parse(input), result.outputRecord());
    assertEquals(NO_FIELDS_EXTRACTED, result.targetOutcome());
    assertEquals(Set.of(), result.extractedKeys());
    assertEquals(remove ? Map.of() : input.unwrap(), result.outputRecord().unwrap());
    assertEquals(Map.of("message", "startup"), input.unwrap());
  }

  @Test
  void identicalOverwriteStillCountsAsExtraction() {
    var input = record(Map.of("message", "host=inner", "host", "inner"));
    var result = kv("message", null, false).parseDetailed(input);
    assertEquals(input, result.outputRecord());
    assertEquals(EXTRACTED, result.targetOutcome());
    assertEquals(Set.of("host"), result.extractedKeys());
  }

  @Test
  void jsonExtractionReportsTheConfiguredOutputKey() {
    var rule =
        new ParserConfigCompiler()
            .compile(
                ParserConfigs.ParserConfig.builder()
                    .targetField("message")
                    .extractionConfig(new JsonExtractionConfig("payload"))
                    .build());
    var input = record(Map.of("message", "{\"host\":\"inner\"}"));
    var result = rule.parseDetailed(input);
    assertEquals(rule.parse(input), result.outputRecord());
    assertEquals(Set.of("payload"), result.extractedKeys());
    assertEquals(Map.of("host", "inner"), result.outputRecord().unwrap().get("payload"));
  }

  @ParameterizedTest
  @MethodSource("skippedInputs")
  void reportsSkippedInputsWithoutRunningExtractionOrDispatch(
      Map<String, Object> payload,
      CompiledRules.InputKind kind,
      CompiledRules.TargetOutcome outcome) {
    var input = record(payload);
    var rule =
        new CompiledRules.ParseRule(
            "message",
            true,
            text -> {
              throw new AssertionError("Must not extract a skipped target");
            },
            new CompiledRules.DispatchRule("message", Map.of(), kv("message", null, false)));
    assertSame(input, rule.parse(input));
    var result = rule.parseDetailed(input);
    assertSame(input, result.outputRecord());
    assertEquals(outcome, result.targetOutcome());
    assertEquals(1, result.events().size());
    assertEquals(kind, result.events().getFirst().inputKind());
    assertFalse(result.events().getFirst().removedTarget());
  }

  static Stream<Arguments> skippedInputs() {
    var nullValue = new HashMap<String, Object>();
    nullValue.put("message", null);
    return Stream.of(
        Arguments.of(Map.of(), MISSING, SKIPPED_MISSING),
        Arguments.of(nullValue, NULL, SKIPPED_NON_TEXT),
        Arguments.of(Map.of("message", ""), EMPTY_TEXT, SKIPPED_EMPTY),
        Arguments.of(Map.of("message", Map.of("x", "y")), OBJECT, SKIPPED_NON_TEXT),
        Arguments.of(Map.of("message", 3), OTHER_NON_TEXT, SKIPPED_NON_TEXT),
        Arguments.of(Map.of("message", false), OTHER_NON_TEXT, SKIPPED_NON_TEXT),
        Arguments.of(Map.of("message", List.of("a")), OTHER_NON_TEXT, SKIPPED_NON_TEXT));
  }

  @Test
  void missingNestedAncestorDoesNotFallBackToLiteralTarget() {
    var input = record(Map.of("payload.message", "host=literal", "payload", "text"));
    var result = kv("payload.message", List.of("payload", "message"), true).parseDetailed(input);
    assertSame(input, result.outputRecord());
    assertEquals(SKIPPED_MISSING, result.targetOutcome());
  }

  @Test
  void whitespaceIsAnExecutedText() {
    assertEquals(
        NO_FIELDS_EXTRACTED,
        kv("message", null, false).parseDetailed(record(Map.of("message", "  "))).targetOutcome());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void dispatchRunsOnceAndKeepsRuleOrder(boolean useDefault) {
    var calls = new AtomicInteger();
    var next =
        new CompiledRules.ParseRule(
            "message",
            true,
            text -> {
              calls.incrementAndGet();
              return record(Map.of("value", text));
            },
            null,
            List.of("payload", "message"));
    var dispatch =
        new CompiledRules.DispatchRule(
            "type", useDefault ? Map.of() : Map.of("a", next), useDefault ? next : null);
    var rule =
        new CompiledRules.ParseRule(
            "message",
            true,
            text -> {
              calls.incrementAndGet();
              return record(Map.of("type", "a", "payload", Map.of("message", text)));
            },
            dispatch);
    var input = record(Map.of("message", "text"));
    var result = rule.parseDetailed(input);
    assertEquals(2, calls.get());
    assertEquals(2, result.events().size());
    assertEquals(List.of(), result.events().getFirst().rulePath());
    assertEquals(
        useDefault
            ? List.of("dispatchConfig", "defaultConfig")
            : List.of("dispatchConfig", "dispatchMap", "a"),
        result.events().get(1).rulePath());
    assertEquals(Set.of("value"), result.events().get(1).extractedKeys());
    assertEquals(rule.parse(input), result.outputRecord());
    assertEquals(4, calls.get());
    assertEquals(Map.of("message", "text"), input.unwrap());
  }

  @Test
  void failureKeepsOriginalCauseAndFailedDispatchPosition() {
    var cause = new IllegalArgumentException("invalid text");
    var next =
        new CompiledRules.ParseRule(
            "content",
            false,
            text -> {
              throw cause;
            },
            null);
    var rule =
        new CompiledRules.ParseRule(
            "message",
            false,
            text -> record(Map.of("content", text)),
            new CompiledRules.DispatchRule("content", Map.of(), next));
    var input = record(Map.of("message", "startup"));
    var ordinary = assertThrows(RuntimeException.class, () -> rule.parse(input));
    var detailed =
        assertThrows(CompiledRules.DetailedParseException.class, () -> rule.parseDetailed(input));
    assertSame(cause, ordinary.getCause());
    assertSame(cause, detailed.getCause());
    assertEquals(ordinary.getMessage(), detailed.getMessage());
    assertEquals(List.of("dispatchConfig", "defaultConfig"), detailed.getRulePath());
    assertEquals(List.of("content"), detailed.getTargetFieldPath());
    assertEquals(TEXT, detailed.getInputKind());
    assertEquals(1, detailed.getEvents().size());
  }

  @Test
  void nonStringDispatchKeepsExistingFailureSemantics() {
    var rule =
        new CompiledRules.ParseRule(
            "message",
            false,
            text -> record(Map.of("type", Map.of("x", "y"))),
            new CompiledRules.DispatchRule("type", Map.of(), kv("message", null, false)));
    var input = record(Map.of("message", "text"));
    var ordinary = assertThrows(UnsupportedOperationException.class, () -> rule.parse(input));
    var detailed =
        assertThrows(CompiledRules.DetailedParseException.class, () -> rule.parseDetailed(input));
    assertEquals(ordinary.getMessage(), detailed.getMessage());
    assertInstanceOf(UnsupportedOperationException.class, detailed.getCause());
    assertEquals(List.of(), detailed.getRulePath());
  }

  private static CompiledRules.ParseRule kv(String target, List<String> path, boolean remove) {
    return new ParserConfigCompiler()
        .compile(
            ParserConfigs.ParserConfig.builder()
                .targetField(target)
                .targetFieldPath(path)
                .removeTargetField(remove)
                .extractionConfig(new KvPairExtractionConfig(" ", "="))
                .build());
  }

  private static RecordFleakData record(Map<String, ?> value) {
    return (RecordFleakData) FleakData.wrap(value);
  }
}
