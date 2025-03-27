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
package io.fleak.zephflow.lib.serdes;

import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.util.Map;
import java.util.stream.Collectors;

/** Created by bolei on 9/16/24 */
public record TypedEventContainer<T>(T payload, Map<String, String> metadataWithKey) {
  public Map<String, StringPrimitiveFleakData> getMetadataPayload() {

    if (metadataWithKey == null) {
      return null;
    }

    return metadataWithKey.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> {
                  String v = e.getValue();
                  return new StringPrimitiveFleakData(v);
                }));
  }
}
