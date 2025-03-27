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
package io.fleak.zephflow.lib.parser.extractions;

import static io.fleak.zephflow.lib.parser.extractions.SyslogExtractionConfig.EXTRACTOR_MAP;
import static io.fleak.zephflow.lib.parser.extractions.SyslogHeaderComponentExtractor.skipWhitespace;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.HashMap;
import java.util.Map;

/** Created by bolei on 2/24/25 */
public record SyslogExtractionRule(SyslogExtractionConfig config) implements ExtractionRule {

  public static final String PRIORITY_KEY = "priority";
  public static final String VERSION_KEY = "version";
  public static final String TIMESTAMP_KEY = "timestamp";
  public static final String DEVICE_KEY = "deviceId";
  public static final String APP_NAME_KEY = "appName";
  public static final String PROC_ID_KEY = "procId";
  public static final String MSG_ID_KEY = "msgId";
  public static final String STRUCTURED_DATA_KEY = "structuredData";

  public static final String LOG_CONTENT_KEY = "content";

  @Override
  public RecordFleakData extract(String logEntry) throws Exception {
    if (logEntry == null || logEntry.isEmpty()) {
      throw new IllegalArgumentException("Log entry cannot be null or empty");
    }

    Map<String, Object> result = new HashMap<>();
    logEntry = logEntry.trim();

    // Parse the header using the selected strategy

    int pos = 0;
    for (int i = 0; i < config.componentList.size(); ++i) {
      var comp = config.componentList.get(i);
      var compExtr = EXTRACTOR_MAP.get(comp);
      pos =
          compExtr.extractComponent(
              result,
              logEntry,
              pos,
              i == config.componentList.size() - 1,
              config.getMessageBodyDelimiter(),
              config.timestampPattern);
    }

    pos = consumeMessageBodyDelimiter(logEntry, pos);

    String remaining = pos < logEntry.length() ? logEntry.substring(pos) : "";
    result.put(LOG_CONTENT_KEY, remaining);
    return (RecordFleakData) FleakData.wrap(result);
  }

  private int consumeMessageBodyDelimiter(String logEntry, int startPosition) {
    int pos = skipWhitespace(logEntry, startPosition);
    if (config.getMessageBodyDelimiter() != null) {
      Preconditions.checkArgument(logEntry.charAt(pos) == config.getMessageBodyDelimiter());
      pos += 1;
    }
    pos = skipWhitespace(logEntry, pos);
    return pos;
  }
}
