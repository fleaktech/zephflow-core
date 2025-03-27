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
package io.fleak.zephflow.lib.commands.assertion;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Created by bolei on 11/13/24 */
class AssertionCommandTest {

  private FleakCounter inputMessageCounter;
  private FleakCounter outputMessageCounter;
  private FleakCounter errorCounter;
  private MetricClientProvider metricClientProvider;

  @BeforeEach
  public void setup() {
    metricClientProvider = mock();
    inputMessageCounter = mock();
    outputMessageCounter = mock();
    errorCounter = mock();
    when(metricClientProvider.counter(eq(METRIC_NAME_INPUT_EVENT_COUNT), any()))
        .thenReturn(inputMessageCounter);
    when(metricClientProvider.counter(eq(METRIC_NAME_OUTPUT_EVENT_COUNT), any()))
        .thenReturn(outputMessageCounter);
    when(metricClientProvider.counter(eq(METRIC_NAME_ERROR_EVENT_COUNT), any()))
        .thenReturn(errorCounter);
  }

  @Test
  void process_BooleanExpression() {
    AssertionCommandFactory assertionCommandFactory = new AssertionCommandFactory(true);
    AssertionCommand assertionCommand =
        (AssertionCommand) assertionCommandFactory.createCommand("my_node", TestUtils.JOB_CONTEXT);
    assertionCommand.parseAndValidateArg("$.foo %2 == 0 ");

    RecordFleakData r0 = (RecordFleakData) FleakData.wrap(Map.of("foo", 0));
    RecordFleakData r1 = (RecordFleakData) FleakData.wrap(Map.of("foo", 1));

    assert r0 != null;
    assert r1 != null;
    ScalarCommand.ProcessResult processResult =
        assertionCommand.process(List.of(r0, r1), "test_user", metricClientProvider);

    assertEquals(List.of(r0), processResult.getOutput());
    assertEquals(
        List.of(new ErrorOutput(r1, "assertion failed: " + toJsonString(r1.unwrap()))),
        processResult.getFailureEvents());

    verify(inputMessageCounter, times(2)).increase(any());
    verify(outputMessageCounter).increase(any());
    verify(errorCounter).increase(any());
  }

  @Test
  public void process_NonBooleanExpression() {
    AssertionCommandFactory assertionCommandFactory = new AssertionCommandFactory(true);
    AssertionCommand assertionCommand =
        (AssertionCommand) assertionCommandFactory.createCommand("my_node", TestUtils.JOB_CONTEXT);
    assertionCommand.parseAndValidateArg("dict(bar=$.foo)");
    RecordFleakData r0 = (RecordFleakData) FleakData.wrap(Map.of("foo", 0));
    RecordFleakData r1 = (RecordFleakData) FleakData.wrap(Map.of("foo", 1));
    assert r0 != null;
    assert r1 != null;
    ScalarCommand.ProcessResult processResult =
        assertionCommand.process(List.of(r0, r1), "test_user", metricClientProvider);

    assertTrue(processResult.getOutput().isEmpty());
    assertEquals(
        List.of(
            new ErrorOutput(r0, "assertion failed: " + toJsonString(r0.unwrap())),
            new ErrorOutput(r1, "assertion failed: " + toJsonString(r1.unwrap()))),
        processResult.getFailureEvents());

    verify(inputMessageCounter, times(2)).increase(any());
    verify(outputMessageCounter, never()).increase(any());
    verify(errorCounter, times(2)).increase(any());
  }

  @Test
  public void process_filter() {
    AssertionCommandFactory assertionCommandFactory = new AssertionCommandFactory(false);
    AssertionCommand assertionCommand =
        (AssertionCommand) assertionCommandFactory.createCommand("my_node", TestUtils.JOB_CONTEXT);
    assertionCommand.parseAndValidateArg("$.foo %2 == 0 ");
    RecordFleakData r0 = (RecordFleakData) FleakData.wrap(Map.of("foo", 0));
    RecordFleakData r1 = (RecordFleakData) FleakData.wrap(Map.of("foo", 1));

    assert r0 != null;
    assert r1 != null;
    ScalarCommand.ProcessResult processResult =
        assertionCommand.process(List.of(r0, r1), "test_user", metricClientProvider);
    assertEquals(List.of(r0), processResult.getOutput());
    assertTrue(processResult.getFailureEvents().isEmpty());

    verify(inputMessageCounter, times(2)).increase(any());
    verify(outputMessageCounter).increase(any());
    verify(errorCounter, never()).increase(any());
  }
}
