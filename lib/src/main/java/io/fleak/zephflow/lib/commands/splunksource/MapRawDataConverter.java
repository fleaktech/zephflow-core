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

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MapRawDataConverter implements RawDataConverter<Map<String, String>> {

  @Override
  public ConvertedResult<Map<String, String>> convert(
      Map<String, String> sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    try {
      RecordFleakData record = (RecordFleakData) FleakData.wrap(sourceRecord);

      Map<String, String> eventTags = getCallingUserTagAndEventTags(null, record);

      sourceInitializedConfig.dataSizeCounter().increase(1, eventTags);
      sourceInitializedConfig.inputEventCounter().increase(1, eventTags);
      if (log.isTraceEnabled()) {
        log.trace("Converted Splunk event: {}", toJsonString(record));
      }

      return ConvertedResult.success(List.of(record), sourceRecord);
    } catch (Exception e) {
      sourceInitializedConfig.deserializeFailureCounter().increase(Map.of());
      log.error("Failed to convert Splunk event: {}", sourceRecord, e);
      return ConvertedResult.failure(e, sourceRecord);
    }
  }
}
