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
package io.fleak.zephflow.lib.commands.imapsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EmailRawDataConverter implements RawDataConverter<EmailMessage> {

  @Override
  public ConvertedResult<EmailMessage> convert(
      EmailMessage sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    try {
      Map<String, Object> map = toMap(sourceRecord);
      RecordFleakData record = (RecordFleakData) FleakData.wrap(map);
      List<RecordFleakData> events = List.of(record);

      Map<String, String> eventTags = getCallingUserTagAndEventTags(null, record);
      sourceInitializedConfig.inputEventCounter().increase(events.size(), eventTags);
      sourceInitializedConfig.dataSizeCounter().increase(estimateSize(sourceRecord), eventTags);

      return ConvertedResult.success(events, sourceRecord);
    } catch (Exception e) {
      sourceInitializedConfig.deserializeFailureCounter().increase(Map.of());
      log.error("failed to convert email message", e);
      return ConvertedResult.failure(e, sourceRecord);
    }
  }

  static Map<String, Object> toMap(EmailMessage email) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("messageId", email.messageId());
    map.put("from", email.from());
    map.put("to", email.to());
    map.put("cc", email.cc());
    map.put("subject", email.subject());
    map.put("bodyText", email.bodyText());
    map.put("bodyHtml", email.bodyHtml());
    map.put("headers", email.headers());

    if (email.attachments() != null && !email.attachments().isEmpty()) {
      List<Map<String, Object>> attachmentList = new ArrayList<>();
      for (EmailMessage.Attachment att : email.attachments()) {
        Map<String, Object> attMap = new LinkedHashMap<>();
        attMap.put("filename", att.filename());
        attMap.put("contentType", att.contentType());
        attMap.put("contentBase64", att.contentBase64());
        attachmentList.add(attMap);
      }
      map.put("attachments", attachmentList);
    } else {
      map.put("attachments", List.of());
    }

    map.put("receivedDate", email.receivedDate() != null ? email.receivedDate().toString() : null);
    return map;
  }

  private int estimateSize(EmailMessage email) {
    int size = 0;
    if (email.bodyText() != null) {
      size += email.bodyText().length();
    }
    if (email.bodyHtml() != null) {
      size += email.bodyHtml().length();
    }
    if (email.subject() != null) {
      size += email.subject().length();
    }
    return Math.max(size, 1);
  }
}
