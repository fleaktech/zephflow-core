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
package io.fleak.zephflow.lib.commands.noop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ResourceLock(Resources.SYSTEM_OUT)
class NoopCommandTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void privateBoundedRecordsStayOutOfStdoutWhileOrdinaryOutputIsPreserved(boolean bounded)
      throws Exception {
    var command =
        (NoopCommand) new NoopCommandFactory().createCommand("copy", JobContext.builder().build());
    command.parseAndValidateArg(Map.of());
    if (bounded)
      command.setExecutionHooks(
          new ExecutionHooks(() -> {}, mock(ExecutionHooks.Effects.class), Runnable::run));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var record = (RecordFleakData) FleakData.wrap(Map.of("payload", "private-record-marker"));
    var bytes = new ByteArrayOutputStream();
    var original = System.out;
    try (var capture = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
      System.setOut(capture);
      var result = command.process(List.of(record), "user", command.getExecutionContext());
      assertEquals(List.of(record), result.getOutput());
      assertTrue(result.getFailureEvents().isEmpty());
      if (bounded) assertEquals("", bytes.toString(StandardCharsets.UTF_8));
      else assertTrue(bytes.toString(StandardCharsets.UTF_8).contains("private-record-marker"));
    } finally {
      System.setOut(original);
      command.terminate();
    }
  }
}
