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

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MapRawDataConverterTest {

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

    RecordFleakData record = result.transformedData().get(0);
    assertNotNull(record);
    assertEquals(3, record.getPayload().size());
    assertEquals("server1", record.getPayload().get("host").unwrap());
    assertEquals("/var/log/app.log", record.getPayload().get("source").unwrap());
    assertEquals("Log message", record.getPayload().get("_raw").unwrap());
  }
}
