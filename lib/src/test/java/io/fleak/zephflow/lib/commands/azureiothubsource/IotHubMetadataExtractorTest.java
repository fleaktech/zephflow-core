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

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.models.EventContext;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IotHubMetadataExtractorTest {

  private static EventContext eventContext(
      Map<String, Object> systemProperties, Instant enqueued, Map<String, Object> appProperties) {
    EventData eventData = mock(EventData.class);
    when(eventData.getSystemProperties()).thenReturn(systemProperties);
    when(eventData.getEnqueuedTime()).thenReturn(enqueued);
    when(eventData.getProperties()).thenReturn(appProperties);
    EventContext ctx = mock(EventContext.class);
    when(ctx.getEventData()).thenReturn(eventData);
    return ctx;
  }

  @Test
  void extractsDeviceIdEnqueuedTimeAndProperties() {
    EventContext ctx =
        eventContext(
            Map.of("iothub-connection-device-id", "sensor-01"),
            Instant.parse("2026-07-16T10:00:00Z"),
            Map.of("schema", "v2"));

    Map<String, String> metadata = new IotHubMetadataExtractor().apply(ctx);

    assertEquals("sensor-01", metadata.get("deviceId"));
    assertEquals("2026-07-16T10:00:00Z", metadata.get("enqueuedTime"));
    assertEquals("v2", metadata.get("prop.schema"));
  }

  @Test
  void returnsNullWhenNothingPresent() {
    EventContext ctx = eventContext(Map.of(), null, Map.of());
    assertNull(new IotHubMetadataExtractor().apply(ctx));
  }
}
