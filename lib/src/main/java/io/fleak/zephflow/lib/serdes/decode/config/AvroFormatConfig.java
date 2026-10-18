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
package io.fleak.zephflow.lib.serdes.decode.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.Locale;

/**
 * {@code schema} is the writer schema for datum mode, as a JSON object or a string holding one; an
 * object container file carries its own schema.
 */
public record AvroFormatConfig(Container container, JsonNode schema) implements FormatConfig {

  public enum Container {
    /** An object container file. */
    OCF,
    /** The whole input is one binary-encoded record. */
    DATUM;

    @JsonValue
    public String configName() {
      return name().toLowerCase(Locale.ROOT);
    }

    @JsonCreator
    public static Container fromConfigName(String name) {
      return Arrays.stream(values())
          .filter(container -> container.configName().equalsIgnoreCase(name))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("unknown avro container: " + name));
    }
  }
}
