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

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.lib.serdes.encode.EncodingCompiler;
import io.fleak.zephflow.lib.serdes.encode.EncodingConfig;
import io.fleak.zephflow.lib.serdes.encode.RecordEncoder;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability;
import java.nio.charset.StandardCharsets;

/** How a byte-writing sink picks its output: its {@code encoding}. */
public final class SinkEncoding {

  /** Without {@code encoding}, sqssink and pubsubsink write each record as a JSON object. */
  public static final RecordEncoder JSON_OBJECT =
      record -> toJsonString(record).getBytes(StandardCharsets.UTF_8);

  private SinkEncoding() {}

  /**
   * Checks {@code encoding} against the sink.
   *
   * @throws IllegalArgumentException when {@code encoding} is not set or the sink cannot write it
   */
  public static void validate(EncodingConfig encoding, WriterCapability capability) {
    if (encoding == null) {
      throw ConfigurationValidationException.at(
          ConfigurationPath.of("encoding"),
          new IllegalArgumentException(capability.sink() + ": encoding is required"),
          "Encoding configuration is required.");
    }
    try {
      EncodingCompiler.validate(encoding, capability);
    } catch (ConfigurationValidationException failure) {
      throw failure.withPrefix(ConfigurationPath.of("encoding"));
    }
  }
}
