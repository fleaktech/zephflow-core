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

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.util.LinkedHashMap;
import java.util.Map;

public final class FormatRecord {
  public static final String ENVELOPE = "__format__";
  public static final String FORMAT_PATH = "$." + ENVELOPE + ".id";

  private FormatRecord() {}

  public static RecordFleakData create(DetectedFormat format, String text, RecordFleakData record) {
    Map<String, FleakData> content = new LinkedHashMap<>();
    content.put("id", new StringPrimitiveFleakData(format.id()));
    content.put("text", new StringPrimitiveFleakData(text));
    content.put("record", record);
    content.put("metadata", new RecordFleakData(new LinkedHashMap<>()));
    return new RecordFleakData(new LinkedHashMap<>(Map.of(ENVELOPE, new RecordFleakData(content))));
  }

  public static RecordFleakData structured(RecordFleakData record) {
    return create(DetectedFormat.JSON_OBJECT, "", record);
  }

  public static DetectedFormat format(RecordFleakData record) {
    FleakData value = content(record).getPayload().get("id");
    if (!(value instanceof StringPrimitiveFleakData text)) {
      throw new IllegalArgumentException("Format envelope requires a format id");
    }
    return DetectedFormat.fromId(text.getStringValue());
  }

  public static String text(RecordFleakData record) {
    FleakData value = content(record).getPayload().get("text");
    if (!(value instanceof StringPrimitiveFleakData text)) {
      throw new IllegalArgumentException("Format envelope requires original text");
    }
    return text.getStringValue();
  }

  public static RecordFleakData project(RecordFleakData envelope) {
    Map<String, FleakData> payload = new LinkedHashMap<>();
    if (format(envelope) == DetectedFormat.JSON_OBJECT) {
      FleakData record = content(envelope).getPayload().get("record");
      if (!(record instanceof RecordFleakData object)) {
        throw new IllegalArgumentException("JSON Object envelope requires an object");
      }
      payload.putAll(object.getPayload());
    } else {
      payload.put("__raw__", new StringPrimitiveFleakData(text(envelope)));
    }
    payload.putAll(metadata(envelope).getPayload());
    return new RecordFleakData(payload);
  }

  public static RecordFleakData withMetadata(
      RecordFleakData envelope, Map<String, FleakData> additions) {
    Map<String, FleakData> values = new LinkedHashMap<>(metadata(envelope).getPayload());
    values.putAll(additions);
    Map<String, FleakData> content = new LinkedHashMap<>(content(envelope).getPayload());
    content.put("metadata", new RecordFleakData(values));
    return new RecordFleakData(new LinkedHashMap<>(Map.of(ENVELOPE, new RecordFleakData(content))));
  }

  private static RecordFleakData content(RecordFleakData envelope) {
    if (envelope.getPayload().size() != 1
        || !(envelope.getPayload().get(ENVELOPE) instanceof RecordFleakData content)) {
      throw new IllegalArgumentException("Expected a controlled format envelope");
    }
    return content;
  }

  private static RecordFleakData metadata(RecordFleakData envelope) {
    if (!(content(envelope).getPayload().get("metadata") instanceof RecordFleakData metadata)) {
      throw new IllegalArgumentException("Format envelope requires metadata");
    }
    return metadata;
  }
}
