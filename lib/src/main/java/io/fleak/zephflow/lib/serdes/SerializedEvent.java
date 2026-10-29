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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;

/** Created by bolei on 9/16/24 */
public record SerializedEvent(byte[] key, byte[] value, Map<String, String> metadata) {

  public SerializedEvent copyWithValue(byte[] value) {
    return new SerializedEvent(key, value, metadata);
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
  public @NonNull String toString() {
    return String.format(
        "%s%n%s%nmetadata:%n%s",
        describe("key", key), describe("value", value), toJsonString(metadata));
  }

  private static String describe(String name, byte[] bytes) {
    if (bytes == null) {
      return name + ":\nnull";
    }
    String text = readableText(bytes);
    return text != null ? name + "(text):\n" + text : name + "(base64):\n" + toBase64String(bytes);
  }

  /** The bytes as UTF-8 text, or null when they are not valid UTF-8 or hold control characters. */
  private static String readableText(byte[] bytes) {
    String text;
    try {
      text = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
    } catch (CharacterCodingException e) {
      return null;
    }
    boolean readable =
        text.chars()
            .allMatch(c -> !Character.isISOControl(c) || c == '\n' || c == '\r' || c == '\t');
    return readable ? text : null;
  }
}
