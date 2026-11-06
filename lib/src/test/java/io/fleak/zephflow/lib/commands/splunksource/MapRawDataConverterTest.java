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
package io.fleak.zephflow.lib.commands.splunksource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData.NumberType;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MapRawDataConverterTest {

  private MapRawDataConverter converter;
  private SourceExecutionContext<?> mockContext;
  private FleakCounter mockInputEventCounter;
  private FleakCounter mockDataSizeCounter;
  private FleakCounter mockDeserializeFailureCounter;

  @BeforeEach
  void setUp() {
    converter = new MapRawDataConverter();
    mockContext = mock(SourceExecutionContext.class);
    mockInputEventCounter = mock(FleakCounter.class);
    mockDataSizeCounter = mock(FleakCounter.class);
    mockDeserializeFailureCounter = mock(FleakCounter.class);

    when(mockContext.inputEventCounter()).thenReturn(mockInputEventCounter);
    when(mockContext.dataSizeCounter()).thenReturn(mockDataSizeCounter);
    when(mockContext.deserializeFailureCounter()).thenReturn(mockDeserializeFailureCounter);
  }

  @Test
  void testMapRawDataConverter() {
    var converter = new MapRawDataConverter();
    var sourceRecord = new HashMap<String, String>();
    sourceRecord.put("host", "server1");
    sourceRecord.put("source", "/var/log/app.log");
    sourceRecord.put("_raw", "Log message");

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

    var result = converter.convert(sourceRecord, context);

    assertNotNull(result);
    assertNotNull(result.transformedData());
    assertEquals(1, result.transformedData().size());

    assertEquals(
        List.of(
            record(
                Map.of(
                    "host", str("server1"),
                    "source", str("/var/log/app.log"),
                    "_raw", str("Log message"),
                    "__host__", str("server1"),
                    "__source__", str("/var/log/app.log")))),
        result.transformedData());
  }

  private static StringPrimitiveFleakData str(String value) {
    return new StringPrimitiveFleakData(value);
  }

  private static RecordFleakData record(Map<String, FleakData> payload) {
    return new RecordFleakData(new HashMap<>(payload));
  }

  private List<RecordFleakData> convert(Map<String, String> result) {
    return converter.convert(new HashMap<>(result), mockContext).transformedData();
  }

  @Test
  void convert_fullResultGetsLabelsButNoIndex() {
    assertEquals(
        List.of(
            record(
                Map.of(
                    "_raw", str("line"),
                    "_time", str("2024-05-01T10:00:00.250-07:00"),
                    "host", str("h1"),
                    "source", str("/var/log/a"),
                    "sourcetype", str("st"),
                    "index", str("main"),
                    "__host__", str("h1"),
                    "__source__", str("/var/log/a"),
                    "__sourcetype__", str("st"),
                    "__ts__", new NumberPrimitiveFleakData(1714582800250L, NumberType.LONG)))),
        convert(
            Map.of(
                "_raw", "line",
                "_time", "2024-05-01T10:00:00.250-07:00",
                "host", "h1",
                "source", "/var/log/a",
                "sourcetype", "st",
                "index", "main")));
  }

  @Test
  void convert_unparseableTimeLeavesTimestampUnset() {
    assertEquals(
        List.of(
            record(
                Map.of(
                    "_time", str("not a time"),
                    "sourcetype", str("st"),
                    "__sourcetype__", str("st")))),
        convert(Map.of("_time", "not a time", "sourcetype", "st")));
  }

  @Test
  void convert_missingOrEmptyFieldsLeaveLabelsUnset() {
    assertEquals(
        List.of(record(Map.of("_raw", str("line"), "source", str("")))),
        convert(Map.of("_raw", "line", "source", "")));
  }

  @Test
  void convert_keepsALabelTheSearchAlreadyReturned() {
    assertEquals(
        List.of(record(Map.of("host", str("h1"), "__host__", str("computed")))),
        convert(Map.of("host", "h1", "__host__", "computed")));
  }

  @Test
  void testConvert_verifiesMetricTagsAreExtractedAndPassed() {
    // Arrange: Create a simple Splunk event
    Map<String, String> sourceRecord = new HashMap<>();
    sourceRecord.put("host", "server1");
    sourceRecord.put("_raw", "test log");

    // Act
    ConvertedResult<Map<String, String>> result = converter.convert(sourceRecord, mockContext);

    // Assert
    assertNull(result.error());
    assertEquals(1, result.transformedData().size());

    verify(mockDataSizeCounter).increase(anyLong(), any());
    verify(mockInputEventCounter).increase(eq(1L), any());
    verify(mockDeserializeFailureCounter, never()).increase(any());
  }
}
