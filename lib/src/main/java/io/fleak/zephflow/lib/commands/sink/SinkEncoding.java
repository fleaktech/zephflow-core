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
package io.fleak.zephflow.lib.commands.sink;

import io.fleak.zephflow.lib.serdes.encode.EncodingCompiler;
import io.fleak.zephflow.lib.serdes.encode.EncodingConfig;
import io.fleak.zephflow.lib.serdes.encode.RecordEncoder;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A byte-writing sink picks its output with {@code encoding} or with its legacy format fields
 * ({@code encodingType}, {@code compressionType}, {@code avroSchema}, {@code csvColumns}, the kv
 * options), never both.
 */
public final class SinkEncoding {

  private SinkEncoding() {}

  /** A legacy format field of a sink config, by its config name. */
  public record LegacyField(String name, Object value) {

    boolean isSet() {
      return switch (value) {
        case null -> false;
        case String s -> !s.isBlank();
        case Collection<?> c -> !c.isEmpty();
        case Map<?, ?> m -> !m.isEmpty();
        default -> true;
      };
    }
  }

  public static LegacyField field(String name, Object value) {
    return new LegacyField(name, value);
  }

  /** Refuses {@code encoding} next to any set legacy field, then checks it against the sink. */
  public static void validate(
      EncodingConfig encoding, WriterCapability capability, LegacyField... legacyFields) {
    List<String> set =
        Arrays.stream(legacyFields).filter(LegacyField::isSet).map(LegacyField::name).toList();
    if (!set.isEmpty()) {
      throw new IllegalArgumentException(
          capability.sink() + ": encoding cannot be combined with " + String.join(", ", set));
    }
    EncodingCompiler.validate(encoding, capability);
  }

  /** A legacy serializer as a record encoder: the bytes it writes for one record. */
  public static RecordEncoder fromSerializer(FleakSerializer<?> serializer) {
    return record -> serializer.serialize(List.of(record)).value();
  }
}
