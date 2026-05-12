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
package io.fleak.zephflow.lib.commands.piimask;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static io.fleak.zephflow.lib.utils.JsonUtils.loadFleakDataFromJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_ERROR_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_OUTPUT_EVENT_COUNT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class PiiMaskCommandTest {

  @Test
  void masksEmailWithDefaultTokenInPlace() {
    RecordFleakData input =
        json(
            """
        { "message": "ping a@b.com please" }
        """);

    ScalarCommand.ProcessResult result =
        run(
            Map.of(
                "targets", List.of("$.message"),
                "detectors", Map.of("email", Map.of())),
            input);

    assertTrue(result.getFailureEvents().isEmpty());
    assertEquals(1, result.getOutput().size());
    assertEquals(
        Map.of("message", "ping [EMAIL] please"),
        JsonUtils.OBJECT_MAPPER.convertValue(result.getOutput().get(0).unwrap(), Map.class));
  }

  @Test
  void disabledDetectorIsNotApplied() {
    RecordFleakData input =
        json(
            """
        { "message": "+1 415 555 1234 and a@b.com" }
        """);
    ScalarCommand.ProcessResult result =
        run(
            Map.of(
                "targets", List.of("$.message"),
                "detectors", Map.of("email", Map.of())), // phone NOT enabled
            input);
    String masked = (String) ((Map<?, ?>) result.getOutput().get(0).unwrap()).get("message");
    assertTrue(masked.contains("+1 415 555 1234"));
    assertTrue(masked.contains("[EMAIL]"));
  }

  @Test
  void customReplacementOverridesDefault() {
    RecordFleakData input =
        json(
            """
        { "message": "a@b.com" }
        """);
    ScalarCommand.ProcessResult result =
        run(
            Map.of(
                "targets", List.of("$.message"),
                "detectors", Map.of("email", Map.of("replacement", "<E>"))),
            input);
    assertEquals(
        Map.of("message", "<E>"),
        JsonUtils.OBJECT_MAPPER.convertValue(result.getOutput().get(0).unwrap(), Map.class));
  }

  @Test
  void customPatternIsApplied() {
    RecordFleakData input =
        json(
            """
        { "message": "ref INT-12345678 in the system" }
        """);
    ScalarCommand.ProcessResult result =
        run(
            Map.of(
                "targets", List.of("$.message"),
                "customPatterns",
                    List.of(
                        Map.of(
                            "name", "internalId",
                            "pattern", "INT-\\d{8}",
                            "replacement", "<INT>"))),
            input);
    assertEquals(
        Map.of("message", "ref <INT> in the system"),
        JsonUtils.OBJECT_MAPPER.convertValue(result.getOutput().get(0).unwrap(), Map.class));
  }

  @Test
  void masksAcrossMultipleTargets() {
    RecordFleakData input =
        json(
            """
        { "user": { "email": "a@b.com" }, "note": "ssn 123-45-6789 here" }
        """);
    ScalarCommand.ProcessResult result =
        run(
            Map.of(
                "targets", List.of("$.user.email", "$.note"),
                "detectors",
                    Map.of(
                        "email", Map.of(),
                        "ssn", Map.of())),
            input);
    Map<?, ?> out = (Map<?, ?>) result.getOutput().get(0).unwrap();
    assertEquals("[EMAIL]", ((Map<?, ?>) out.get("user")).get("email"));
    assertEquals("ssn [SSN] here", out.get("note"));
  }

  @Test
  void missingTargetLeavesRecordUntouched() {
    RecordFleakData input =
        json(
            """
        { "message": "a@b.com" }
        """);
    ScalarCommand.ProcessResult result =
        run(
            Map.of(
                "targets", List.of("$.nope"),
                "detectors", Map.of("email", Map.of())),
            input);
    assertEquals(
        Map.of("message", "a@b.com"),
        JsonUtils.OBJECT_MAPPER.convertValue(result.getOutput().get(0).unwrap(), Map.class));
  }

  @Test
  void nonStringScalarTargetSkipped() {
    RecordFleakData input =
        json(
            """
        { "count": 42 }
        """);
    ScalarCommand.ProcessResult result =
        run(
            Map.of(
                "targets", List.of("$.count"),
                "detectors", Map.of("email", Map.of())),
            input);
    assertEquals(
        Map.of("count", 42L),
        JsonUtils.OBJECT_MAPPER.convertValue(result.getOutput().get(0).unwrap(), Map.class));
  }

  @Test
  void arrayOfStringsHasEachElementMasked() {
    RecordFleakData input =
        json(
            """
        { "recipients": ["a@b.com", "c@d.com", "no-pii"] }
        """);
    ScalarCommand.ProcessResult result =
        run(
            Map.of(
                "targets", List.of("$.recipients"),
                "detectors", Map.of("email", Map.of())),
            input);
    Map<?, ?> out = (Map<?, ?>) result.getOutput().get(0).unwrap();
    assertEquals(List.of("[EMAIL]", "[EMAIL]", "no-pii"), out.get("recipients"));
  }

  @Test
  void mixedArrayMasksOnlyStrings() {
    RecordFleakData input =
        json(
            """
        { "mixed": ["a@b.com", 7, "c@d.com"] }
        """);
    ScalarCommand.ProcessResult result =
        run(
            Map.of(
                "targets", List.of("$.mixed"),
                "detectors", Map.of("email", Map.of())),
            input);
    Map<?, ?> out = (Map<?, ?>) result.getOutput().get(0).unwrap();
    assertEquals(List.of("[EMAIL]", 7L, "[EMAIL]"), out.get("mixed"));
  }

  @Test
  void creditCardLuhnValidReplacedInvalidLeftIntact() {
    RecordFleakData input =
        json(
            """
        { "msg": "good 4111-1111-1111-1111 bad 1234-5678-9012-3456" }
        """);
    ScalarCommand.ProcessResult result =
        run(
            Map.of(
                "targets", List.of("$.msg"),
                "detectors", Map.of("creditCard", Map.of())),
            input);
    Map<?, ?> out = (Map<?, ?>) result.getOutput().get(0).unwrap();
    assertEquals("good [CC] bad 1234-5678-9012-3456", out.get("msg"));
  }

  @Test
  void ipv4OctetValidationRejectsOutOfRange() {
    RecordFleakData input =
        json(
            """
        { "msg": "ok 192.168.0.1 bad 999.1.1.1" }
        """);
    ScalarCommand.ProcessResult result =
        run(
            Map.of(
                "targets", List.of("$.msg"),
                "detectors", Map.of("ipv4", Map.of())),
            input);
    Map<?, ?> out = (Map<?, ?>) result.getOutput().get(0).unwrap();
    assertEquals("ok [IPV4] bad 999.1.1.1", out.get("msg"));
  }

  @Test
  void piiMatchCounterIncrementedByReplacementCount() {
    MetricClientProvider mockProvider = mock(MetricClientProvider.class);
    FleakCounter piiMatchCounter = mock(FleakCounter.class);
    when(mockProvider.counter(eq(PiiMaskCommand.METRIC_NAME_PII_MATCH_COUNT), any()))
        .thenReturn(piiMatchCounter);
    when(mockProvider.counter(anyString(), any()))
        .thenReturn(new MetricClientProvider.NoopMetricClientProvider.NoopFleakCounter());
    when(mockProvider.counter(eq(PiiMaskCommand.METRIC_NAME_PII_MATCH_COUNT), any()))
        .thenReturn(piiMatchCounter);

    RecordFleakData input = json("{\"msg\": \"a@b.com and c@d.com\"}");
    PiiMaskCommand cmd =
        (PiiMaskCommand) new PiiMaskCommandFactory().createCommand("n1", JOB_CONTEXT);
    cmd.parseAndValidateArg(
        Map.of("targets", List.of("$.msg"), "detectors", Map.of("email", Map.of())));
    cmd.initialize(mockProvider);
    cmd.process(List.of(input), "tester", cmd.getExecutionContext());

    verify(piiMatchCounter).increase(eq(2L), any());
  }

  @Test
  void errorCounterIncrementedWhenProcessingThrows() {
    MetricClientProvider mockProvider = mock(MetricClientProvider.class);
    FleakCounter errorCounter = mock(FleakCounter.class);
    when(mockProvider.counter(eq(METRIC_NAME_ERROR_EVENT_COUNT), any())).thenReturn(errorCounter);
    when(mockProvider.counter(anyString(), any()))
        .thenReturn(new MetricClientProvider.NoopMetricClientProvider.NoopFleakCounter());
    when(mockProvider.counter(eq(METRIC_NAME_ERROR_EVENT_COUNT), any())).thenReturn(errorCounter);

    PiiMasker.Spec throwingSpec =
        new PiiMasker.Spec(
            "thrower",
            Pattern.compile(".+"),
            "[X]",
            s -> {
              throw new RuntimeException("simulated processing failure");
            });

    PiiMaskCommand cmd =
        new PiiMaskCommand(
            "n1", JOB_CONTEXT, new PiiMaskConfigParser(), new PiiMaskConfigValidator()) {
          @Override
          protected ExecutionContext createExecutionContext(
              MetricClientProvider mp, JobContext jc, CommandConfig cc, String nid) {
            FleakCounter input = mp.counter(METRIC_NAME_INPUT_EVENT_COUNT, Map.of());
            FleakCounter output = mp.counter(METRIC_NAME_OUTPUT_EVENT_COUNT, Map.of());
            FleakCounter error = mp.counter(METRIC_NAME_ERROR_EVENT_COUNT, Map.of());
            FleakCounter match = mp.counter(PiiMaskCommand.METRIC_NAME_PII_MATCH_COUNT, Map.of());
            return new PiiMaskExecutionContext(
                input,
                output,
                error,
                List.of(DottedPath.parse("$.msg")),
                List.of(throwingSpec),
                match);
          }
        };
    cmd.parseAndValidateArg(
        Map.of("targets", List.of("$.msg"), "detectors", Map.of("email", Map.of())));
    cmd.initialize(mockProvider);

    ScalarCommand.ProcessResult result =
        cmd.process(List.of(json("{\"msg\": \"hello\"}")), "tester", cmd.getExecutionContext());

    assertEquals(1, result.getFailureEvents().size());
    verify(errorCounter).increase(any(Map.class));
  }

  // --- helpers ---

  private static RecordFleakData json(String s) {
    return (RecordFleakData) loadFleakDataFromJsonString(s);
  }

  private static ScalarCommand.ProcessResult run(
      Map<String, Object> rawConfig, RecordFleakData input) {
    PiiMaskCommand cmd =
        (PiiMaskCommand) new PiiMaskCommandFactory().createCommand("n1", JOB_CONTEXT);
    cmd.parseAndValidateArg(rawConfig);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    return cmd.process(List.of(input), "tester", cmd.getExecutionContext());
  }
}
