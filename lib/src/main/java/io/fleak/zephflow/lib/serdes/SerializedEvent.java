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

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import java.util.HashMap;
import java.util.Map;

/** Created by bolei on 9/16/24 */
public record SerializedEvent(byte[] key, byte[] value, Map<String, String> metadata) {

  public SerializedEvent updateValue(byte[] bytes) {
    return new SerializedEvent(key, bytes, metadata);
  }

  public static Map<String, String> metadataWithKey(SerializedEvent serializedEvent) {
    if (serializedEvent.metadata == null) {
      return Map.of();
    }
    Map<String, String> metadata = new HashMap<>(serializedEvent.metadata);
    if (serializedEvent.key != null) {
      metadata.put(METADATA_KEY, toBase64String(serializedEvent.key));
    }
    return metadata;
  }

  public static SerializedEvent create(Map<String, String> metadataWithKey, byte[] value) {
    if (metadataWithKey == null) {
      return new SerializedEvent(null, value, null);
    }

    Map<String, String> metadata = new HashMap<>();
    byte[] key = null;

    for (Map.Entry<String, String> e : metadataWithKey.entrySet()) {
      if (METADATA_KEY.equals(e.getKey())) {
        key = fromBase64String(e.getValue());
        continue;
      }
      metadata.put(e.getKey(), e.getValue());
    }
    return new SerializedEvent(key, value, metadata);
  }

  @Override
  public String toString() {
    return String.format(
        "key(base64):%n%s%n value(base64):%n%s%nmetadata:%n%s",
        toBase64String(key), toBase64String(value), toJsonString(metadata));
  }
}
