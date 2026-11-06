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
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_HOST;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_SOURCE;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_SOURCETYPE;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_TS;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class MapRawDataConverter implements RawDataConverter<Map<String, String>> {

  @Override
  public ConvertedResult<Map<String, String>> convert(
      Map<String, String> sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    try {
      RecordFleakData record = (RecordFleakData) FleakData.wrap(withLabels(sourceRecord));

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

  private static Map<String, Object> withLabels(Map<String, String> result) {
    Map<String, Object> payload = new HashMap<>(result);
    copyLabel(result, "host", FIELD_NAME_HOST, payload);
    copyLabel(result, "source", FIELD_NAME_SOURCE, payload);
    copyLabel(result, "sourcetype", FIELD_NAME_SOURCETYPE, payload);
    Long time = epochMillis(result.get("_time"));
    if (time != null) {
      payload.putIfAbsent(FIELD_NAME_TS, time);
    }
    return payload;
  }

  private static void copyLabel(
      Map<String, String> result, String field, String label, Map<String, Object> payload) {
    String value = result.get(field);
    if (StringUtils.isNotEmpty(value)) {
      payload.putIfAbsent(label, value);
    }
  }

  private static Long epochMillis(String time) {
    if (time == null) {
      return null;
    }
    try {
      return OffsetDateTime.parse(time, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          .toInstant()
          .toEpochMilli();
    } catch (DateTimeParseException e) {
      return null;
    }
  }
}
