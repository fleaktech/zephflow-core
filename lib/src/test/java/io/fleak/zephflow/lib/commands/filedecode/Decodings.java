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
package io.fleak.zephflow.lib.commands.filedecode;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.serdes.decode.config.CsvFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FramingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.XmlFormatConfig;
import java.util.List;
import java.util.Map;

/** Common {@code decoding} configurations for source tests. */
public final class Decodings {

  public static final FormatConfig JSON_OBJECT = new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT);
  public static final FormatConfig JSON_ARRAY = new JsonFormatConfig(JsonFormatConfig.Mode.ARRAY);
  public static final FormatConfig JSON_LINES = new JsonFormatConfig(JsonFormatConfig.Mode.LINES);
  public static final FormatConfig TEXT_LINE =
      new TextFormatConfig(null, new FramingConfig.Line(false));
  public static final FormatConfig TEXT = new TextFormatConfig(null, new FramingConfig.Whole());
  public static final FormatConfig CSV =
      new CsvFormatConfig(null, null, null, null, null, null, null, true, null);
  public static final FormatConfig XML = new XmlFormatConfig(XmlFormatConfig.Mode.DOCUMENT, null);

  private Decodings() {}

  public static DecodingConfig uncompressed(FormatConfig format) {
    return new DecodingConfig(List.of(), format);
  }

  public static DecodingConfig autoCompression(FormatConfig format) {
    return new DecodingConfig(List.of(DecompressionCodec.AUTO), format);
  }

  /** {@link #uncompressed} as a raw command config value. */
  public static Map<String, Object> uncompressedConfig(FormatConfig format) {
    return OBJECT_MAPPER.convertValue(uncompressed(format), new TypeReference<>() {});
  }

  /** {@link #autoCompression} as a raw command config value. */
  public static Map<String, Object> autoCompressionConfig(FormatConfig format) {
    return OBJECT_MAPPER.convertValue(autoCompression(format), new TypeReference<>() {});
  }

  public static DecodingConfig gzip(FormatConfig format) {
    return new DecodingConfig(List.of(DecompressionCodec.GZIP), format);
  }

  /** {@link #gzip} as a raw command config value. */
  public static Map<String, Object> gzipConfig(FormatConfig format) {
    return OBJECT_MAPPER.convertValue(gzip(format), new TypeReference<>() {});
  }
}
