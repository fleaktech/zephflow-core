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
package io.fleak.zephflow.lib.commands.azureblobsource;

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
public class AzureBlobRawDataConverter implements RawDataConverter<AzureBlobData> {

  private final FleakDeserializer<?> fleakDeserializer;

  public AzureBlobRawDataConverter(FleakDeserializer<?> fleakDeserializer) {
    this.fleakDeserializer = fleakDeserializer;
  }

  @Override
  public ConvertedResult<AzureBlobData> convert(
      AzureBlobData sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    try {
      SerializedEvent serializedEvent =
          new SerializedEvent(null, sourceRecord.content(), sourceRecord.metadata());
      List<RecordFleakData> events = fleakDeserializer.deserialize(serializedEvent);

      Map<String, String> eventTags =
          getCallingUserTagAndEventTags(null, events.isEmpty() ? null : events.getFirst());

      sourceInitializedConfig
          .dataSizeCounter()
          .increase(sourceRecord.content().length, eventTags);
      sourceInitializedConfig.inputEventCounter().increase(events.size(), eventTags);

      return ConvertedResult.success(events, sourceRecord);
    } catch (Exception e) {
      sourceInitializedConfig
          .dataSizeCounter()
          .increase(sourceRecord.content().length, Map.of());
      sourceInitializedConfig.deserializeFailureCounter().increase(Map.of());
      log.error(
          "Failed to deserialize Azure Blob content for blob: {}", sourceRecord.blobName(), e);
      return ConvertedResult.failure(e, sourceRecord);
    }
  }
}
