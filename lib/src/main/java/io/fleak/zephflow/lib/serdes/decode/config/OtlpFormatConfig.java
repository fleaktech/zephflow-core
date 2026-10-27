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
import java.util.Arrays;
import java.util.Locale;

/** One binary OTLP export request of {@code signal} per input. */
public record OtlpFormatConfig(Signal signal) implements FormatConfig {

  public enum Signal {
    LOGS,
    METRICS,
    TRACES;

    @JsonValue
    public String configName() {
      return name().toLowerCase(Locale.ROOT);
    }

    @JsonCreator
    public static Signal fromConfigName(String name) {
      return Arrays.stream(values())
          .filter(signal -> signal.configName().equalsIgnoreCase(name))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("unknown otlp signal: " + name));
    }
  }
}
