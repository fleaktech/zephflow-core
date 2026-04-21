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
package io.fleak.zephflow.lib.commands.azuremonitorsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AzureMonitorSinkMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<AzureMonitorSinkOutboundEvent> {

  private final String timeGeneratedField;

  public AzureMonitorSinkMessageProcessor(String timeGeneratedField) {
    this.timeGeneratedField = timeGeneratedField;
  }

  @Override
  public AzureMonitorSinkOutboundEvent preprocess(RecordFleakData event, long ts) {
    try {
      // Convert RecordFleakData to a Map for JSON serialization
      Map<String, Object> payload =
          new LinkedHashMap<>(OBJECT_MAPPER.convertValue(event, Map.class));

      // Inject TimeGenerated if not already present
      if (!payload.containsKey(timeGeneratedField)) {
        payload.put(timeGeneratedField, DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
      }

      return new AzureMonitorSinkOutboundEvent(OBJECT_MAPPER.writeValueAsString(payload));
    } catch (Exception e) {
      log.error("Failed to preprocess event for Azure Monitor", e);
      // Return minimal JSON so the batch can still be sent
      return new AzureMonitorSinkOutboundEvent(
          "{\""
              + timeGeneratedField
              + "\":\""
              + DateTimeFormatter.ISO_INSTANT.format(Instant.now())
              + "\"}");
    }
  }
}
