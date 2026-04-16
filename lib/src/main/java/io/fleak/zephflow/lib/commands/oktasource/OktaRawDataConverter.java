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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OktaRawDataConverter implements RawDataConverter<OktaLogEvent> {

  private final FleakDeserializer<?> fleakDeserializer;

  public OktaRawDataConverter(FleakDeserializer<?> fleakDeserializer) {
    this.fleakDeserializer = fleakDeserializer;
  }

  @Override
  public ConvertedResult<OktaLogEvent> convert(
      OktaLogEvent sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    try {
      byte[] jsonBytes = OBJECT_MAPPER.writeValueAsBytes(sourceRecord.payload());
      SerializedEvent serializedEvent = new SerializedEvent(null, jsonBytes, Map.of());
      List<RecordFleakData> events = fleakDeserializer.deserialize(serializedEvent);

      Map<String, String> eventTags =
          getCallingUserTagAndEventTags(null, events.isEmpty() ? null : events.getFirst());

      sourceInitializedConfig.dataSizeCounter().increase(jsonBytes.length, eventTags);
      sourceInitializedConfig.inputEventCounter().increase(events.size(), eventTags);

      return ConvertedResult.success(events, sourceRecord);
    } catch (Exception e) {
      sourceInitializedConfig.deserializeFailureCounter().increase(Map.of());
      log.error("Failed to convert Okta event with id: {}", sourceRecord.eventId(), e);
      return ConvertedResult.failure(e, sourceRecord);
    }
  }
}
