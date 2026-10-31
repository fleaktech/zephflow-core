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
package io.fleak.zephflow.lib.serdes.decode.auto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.decode.csv.CsvRecognition;
import io.fleak.zephflow.lib.serdes.decode.json.JsonRecognition;
import io.fleak.zephflow.lib.serdes.decode.yaml.YamlRecognition;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;

public final class FormatDetector {
  private FormatDetector() {}

  public static RecordFleakData classify(String text) {
    if (text == null) {
      throw new IllegalArgumentException("Text is required for format recognition");
    }
    String content = text.startsWith("\uFEFF") ? text.substring(1) : text;
    try {
      JsonNode value = JsonRecognition.document(content);
      if (value != null && !value.isMissingNode()) {
        if (value instanceof ObjectNode object) {
          return FormatRecord.create(
              DetectedFormat.JSON_OBJECT, text, JsonUtils.fromJsonPayload(object));
        }
        if (value.isArray()) {
          return textual(DetectedFormat.JSON_ARRAY, text);
        }
        return textual(DetectedFormat.UNRECOGNIZED_TEXT, text);
      }
    } catch (IOException | IllegalArgumentException exception) {
    }
    String syslogBody = LogRecognition.syslogBody(content);
    if (LogRecognition.cef(content) || (syslogBody != null && LogRecognition.cef(syslogBody))) {
      return textual(DetectedFormat.SYSLOG_CEF, text);
    }
    if (syslogBody != null) {
      return textual(DetectedFormat.SYSLOG_PLAIN, text);
    }
    if (YamlRecognition.matches(content, text.getBytes(StandardCharsets.UTF_8).length)) {
      return textual(DetectedFormat.YAML, text);
    }
    if (CsvRecognition.matches(content)) {
      return textual(DetectedFormat.CSV, text);
    }
    return textual(DetectedFormat.UNRECOGNIZED_TEXT, text);
  }

  private static RecordFleakData textual(DetectedFormat format, String text) {
    return FormatRecord.create(format, text, new RecordFleakData(new LinkedHashMap<>()));
  }
}
