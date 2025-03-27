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
package io.fleak.zephflow.api.structure;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Data;

/**
 * Represents a record-like data structure in Fleak. JSON serialization will output just the payload
 * map.
 */
@Data
@Builder
@JsonSerialize(using = FleakDataSerializer.class)
@JsonDeserialize(using = FleakDataDeserializer.class)
public class RecordFleakData implements FleakData {

  @Builder.Default Map<String, FleakData> payload = new HashMap<>();

  public RecordFleakData() {
    payload = new HashMap<>();
  }

  public RecordFleakData(Map<String, FleakData> payload) {
    this.payload = new HashMap<>(payload);
  }

  public RecordFleakData copy() {
    Map<String, FleakData> newPayload =
        new HashMap<>(Optional.ofNullable(payload).orElse(new HashMap<>()));
    return new RecordFleakData(new HashMap<>(newPayload));
  }

  public RecordFleakData copyAndMerge(Map<String, FleakData> additionalPayload) {
    RecordFleakData copy = this.copy();
    copy.payload.putAll(additionalPayload);
    return copy;
  }

  @Override
  @JsonValue
  public Map<String, Object> unwrap() {
    var m = new HashMap<String, Object>();
    for (var entry : payload.entrySet()) {
      var v = entry.getValue();
      m.put(entry.getKey(), (v == null) ? null : v.unwrap());
    }
    return m;
  }
}
