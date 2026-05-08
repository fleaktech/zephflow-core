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
package io.fleak.zephflow.lib.commands.oktasource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OktaRawDataConverterTest {

  private OktaRawDataConverter converter;
  private SourceExecutionContext<?> mockContext;
  private FleakCounter mockInputEventCounter;
  private FleakCounter mockDataSizeCounter;
  private FleakCounter mockDeserializeFailureCounter;

  @BeforeEach
  void setUp() {
    converter = new OktaRawDataConverter();
    mockContext = mock(SourceExecutionContext.class);
    mockInputEventCounter = mock(FleakCounter.class);
    mockDataSizeCounter = mock(FleakCounter.class);
    mockDeserializeFailureCounter = mock(FleakCounter.class);

    when(mockContext.inputEventCounter()).thenReturn(mockInputEventCounter);
    when(mockContext.dataSizeCounter()).thenReturn(mockDataSizeCounter);
    when(mockContext.deserializeFailureCounter()).thenReturn(mockDeserializeFailureCounter);
  }

  @Test
  void convertsOktaEventPayloadToRecordFleakData() {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("uuid", "evt-1");
    payload.put("eventType", "user.session.start");
    payload.put("severity", "INFO");
    OktaLogEvent event = new OktaLogEvent(payload, "evt-1");

    var metricProvider = new MetricClientProvider.NoopMetricClientProvider();
    var context =
        new SourceExecutionContext<>(
            null,
            null,
            null,
            metricProvider.counter("data_size", Map.of()),
            metricProvider.counter("input_event", Map.of()),
            metricProvider.counter("deser_error", Map.of()),
            null);

    ConvertedResult<OktaLogEvent> result = converter.convert(event, context);

    assertNotNull(result);
    assertNull(result.error());
    assertEquals(1, result.transformedData().size());

    RecordFleakData record = result.transformedData().get(0);
    assertEquals("evt-1", record.getPayload().get("uuid").unwrap());
    assertEquals("user.session.start", record.getPayload().get("eventType").unwrap());
    assertEquals("INFO", record.getPayload().get("severity").unwrap());
  }

  @Test
  void incrementsMetricsOnSuccess() {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("uuid", "evt-2");
    payload.put("eventType", "user.session.end");
    OktaLogEvent event = new OktaLogEvent(payload, "evt-2");

    ConvertedResult<OktaLogEvent> result = converter.convert(event, mockContext);

    assertNull(result.error());
    assertEquals(1, result.transformedData().size());
    verify(mockInputEventCounter).increase(eq(1L), any());
    verify(mockDataSizeCounter).increase(anyLong(), any());
    verify(mockDeserializeFailureCounter, never()).increase(any());
  }

  @Test
  void recordsFailureOnNullPayload() {
    OktaLogEvent event = new OktaLogEvent(null, "evt-bad");

    ConvertedResult<OktaLogEvent> result = converter.convert(event, mockContext);

    assertNotNull(result.error());
    verify(mockDeserializeFailureCounter).increase(any());
    verify(mockInputEventCounter, never()).increase(anyLong(), any());
  }
}
