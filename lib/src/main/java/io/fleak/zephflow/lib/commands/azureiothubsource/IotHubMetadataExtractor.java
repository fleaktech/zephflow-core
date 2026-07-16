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

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.models.EventContext;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Extracts IoT Hub message metadata off each {@link EventContext} into the flat {@code
 * Map<String,String>} carried on {@code SerializedEvent.metadata()}. The IoT Hub built-in endpoint
 * exposes the originating device id as the {@code iothub-connection-device-id} system property;
 * enqueued time comes from {@link EventData#getEnqueuedTime()} and user-defined application
 * properties from {@link EventData#getProperties()}. Returns {@code null} when no metadata is
 * present so the converter emits no {@code _iothub} envelope.
 */
public class IotHubMetadataExtractor implements Function<EventContext, Map<String, String>> {

  static final String DEVICE_ID_SYSTEM_PROPERTY = "iothub-connection-device-id";

  @Override
  public Map<String, String> apply(EventContext eventContext) {
    EventData eventData = eventContext.getEventData();
    Map<String, String> metadata = new HashMap<>();

    Map<String, Object> systemProperties = eventData.getSystemProperties();
    if (systemProperties != null) {
      Object deviceId = systemProperties.get(DEVICE_ID_SYSTEM_PROPERTY);
      if (deviceId != null) {
        metadata.put(IotHubRawDataConverter.DEVICE_ID_KEY, String.valueOf(deviceId));
      }
    }

    if (eventData.getEnqueuedTime() != null) {
      metadata.put(
          IotHubRawDataConverter.ENQUEUED_TIME_KEY, eventData.getEnqueuedTime().toString());
    }

    Map<String, Object> properties = eventData.getProperties();
    if (properties != null) {
      for (Map.Entry<String, Object> e : properties.entrySet()) {
        if (e.getValue() != null) {
          metadata.put(
              IotHubRawDataConverter.PROPERTY_PREFIX + e.getKey(), String.valueOf(e.getValue()));
        }
      }
    }

    return metadata.isEmpty() ? null : metadata;
  }
}
