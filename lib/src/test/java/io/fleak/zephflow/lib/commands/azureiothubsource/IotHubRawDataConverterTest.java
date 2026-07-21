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
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IotHubRawDataConverterTest {

  private final FleakDeserializer<?> deserializer =
      DeserializerFactory.createDeserializerFactory(EncodingType.JSON_OBJECT).createDeserializer();

  @SuppressWarnings("unchecked")
  private SourceExecutionContext<SerializedEvent> ctx() {
    SourceExecutionContext<SerializedEvent> ctx = mock(SourceExecutionContext.class);
    when(ctx.dataSizeCounter()).thenReturn(mock(FleakCounter.class));
    when(ctx.inputEventCounter()).thenReturn(mock(FleakCounter.class));
    when(ctx.deserializeFailureCounter()).thenReturn(mock(FleakCounter.class));
    return ctx;
  }

  @Test
  void nestsMetadataUnderIothubAndKeepsBodyFields() {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put(IotHubRawDataConverter.DEVICE_ID_KEY, "sensor-01");
    metadata.put(IotHubRawDataConverter.ENQUEUED_TIME_KEY, "2026-07-16T10:00:00Z");
    metadata.put(IotHubRawDataConverter.PROPERTY_PREFIX + "schema", "v2");

    SerializedEvent event =
        new SerializedEvent(null, "{\"temp\":72}".getBytes(StandardCharsets.UTF_8), metadata);

    ConvertedResult<SerializedEvent> result =
        new IotHubRawDataConverter(deserializer).convert(event, ctx());

    RecordFleakData record = result.transformedData().get(0);
    Map<String, Object> map = record.unwrap();

    assertEquals(72.0, ((Number) map.get("temp")).doubleValue());

    @SuppressWarnings("unchecked")
    Map<String, Object> iothub = (Map<String, Object>) map.get("_iothub");
    assertEquals("sensor-01", iothub.get("deviceId"));
    assertEquals("2026-07-16T10:00:00Z", iothub.get("enqueuedTime"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) iothub.get("properties");
    assertEquals("v2", properties.get("schema"));
  }

  @Test
  void omitsEnvelopeWhenNoMetadata() {
    SerializedEvent event =
        new SerializedEvent(null, "{\"temp\":72}".getBytes(StandardCharsets.UTF_8), null);

    ConvertedResult<SerializedEvent> result =
        new IotHubRawDataConverter(deserializer).convert(event, ctx());

    Map<String, Object> map = result.transformedData().get(0).unwrap();
    assertFalse(map.containsKey("_iothub"));
  }
}
