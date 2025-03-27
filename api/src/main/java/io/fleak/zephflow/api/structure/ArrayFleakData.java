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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;

/**
 * Represents an array-like data structure in Fleak. JSON serialization will output just the
 * arrayPayload.
 */
@Data
@Builder
@JsonSerialize(using = FleakDataSerializer.class)
@JsonDeserialize(using = FleakDataDeserializer.class)
public class ArrayFleakData implements FleakData {
  List<FleakData> arrayPayload;

  public ArrayFleakData() {
    arrayPayload = new ArrayList<>();
  }

  public ArrayFleakData(List<FleakData> arrayPayload) {
    this.arrayPayload = new ArrayList<>(arrayPayload);
  }

  @Override
  @JsonValue
  public List<?> unwrap() {
    return arrayPayload.stream().map(FleakData::unwrap).collect(Collectors.toList());
  }
}
