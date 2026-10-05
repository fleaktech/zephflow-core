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
package io.fleak.zephflow.lib.commands.sample;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SampleBoundedLoggingTest {
  private static final String PRIVATE = "PRIVATE_SAMPLE_RECORD_7711";

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void conditionEvaluationCarriesBoundedContext(boolean bounded) throws Exception {
    var job =
        JobContext.builder().otherProperties(Map.of(JobContext.FLAG_BOUNDED_MODE, bounded)).build();
    var condition = SampleCondition.compile("arr_foreach($.values, elem, parse_int(elem))", 0, job);
    try (var capture =
        new BoundedLogCapture(
            Class.forName("io.fleak.zephflow.lib.commands.eval.ArrForEachFunction"))) {
      assertEquals(
          FleakData.wrap(List.of(1)),
          condition.expression().evaluate(FleakData.wrap(Map.of("values", List.of("1", PRIVATE)))));
      assertFalse(capture.events().isEmpty());
      assertEquals(
          !bounded,
          capture.events().stream()
              .anyMatch(e -> e.getMessage().getFormattedMessage().contains(PRIVATE)));
      capture.events().forEach(e -> assertNull(e.getThrown()));
    } finally {
      SampleCondition.closeAll(List.of(condition));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void conditionCleanupCarriesBoundedContext(boolean bounded) throws Exception {
    var executor = mock(PythonExecutor.class);
    when(executor.boundedDiagnostics()).thenReturn(bounded);
    doThrow(new IllegalStateException(PRIVATE)).when(executor).close();
    try (var capture = new BoundedLogCapture(SampleCondition.class)) {
      var conditions = List.of(new SampleCondition(null, executor));
      if (bounded) {
        var failure =
            assertThrows(IllegalStateException.class, () -> SampleCondition.closeAll(conditions));
        assertFalse(failure.getMessage().contains(PRIVATE));
        assertEquals(PRIVATE, failure.getCause().getMessage());
      } else {
        SampleCondition.closeAll(conditions);
      }
      assertEquals(1, capture.events().size());
      var event = capture.events().getFirst();
      assertFalse(event.getMessage().getFormattedMessage().contains(PRIVATE));
      if (bounded) assertNull(event.getThrown());
      else assertEquals(PRIVATE, event.getThrown().getMessage());
    }
  }
}
