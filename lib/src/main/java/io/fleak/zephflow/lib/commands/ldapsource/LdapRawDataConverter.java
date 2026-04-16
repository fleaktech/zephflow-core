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
package io.fleak.zephflow.lib.commands.ldapsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LdapRawDataConverter implements RawDataConverter<LdapEntry> {

  @Override
  public ConvertedResult<LdapEntry> convert(
      LdapEntry sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    try {
      Map<String, Object> map = new LinkedHashMap<>();
      map.put("dn", sourceRecord.dn());
      for (var attr : sourceRecord.attributes().entrySet()) {
        List<String> values = attr.getValue();
        if (values.size() == 1) {
          map.put(attr.getKey(), values.get(0));
        } else {
          map.put(attr.getKey(), values);
        }
      }

      RecordFleakData record = (RecordFleakData) FleakData.wrap(map);

      Map<String, String> eventTags = getCallingUserTagAndEventTags(null, record);
      sourceInitializedConfig.dataSizeCounter().increase(1, eventTags);
      sourceInitializedConfig.inputEventCounter().increase(1, eventTags);

      return ConvertedResult.success(List.of(record), sourceRecord);
    } catch (Exception e) {
      sourceInitializedConfig.deserializeFailureCounter().increase(Map.of());
      log.error("Failed to convert LDAP entry: {}", sourceRecord, e);
      return ConvertedResult.failure(e, sourceRecord);
    }
  }
}
