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

import io.fleak.zephflow.lib.serdes.decode.config.AutoFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.AvroFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.CsvFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FramingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.KvFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.OtlpFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ProtobufFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.XmlFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.YamlFormatConfig;

/** A source's {@code decoding} configuration, which every decoding source requires. */
public record SourceDecoding(DecodingConfig decoding) {

  /** The format's type name, as {@code decoding} spells it ({@code json}, {@code csv}, ...). */
  public String formatName() {
    return formatName(decodingConfig().format());
  }

  public static String formatName(FormatConfig format) {
    return switch (format) {
      case AutoFormatConfig f -> "auto";
      case AvroFormatConfig f -> "avro";
      case CsvFormatConfig f -> "csv";
      case JsonFormatConfig f -> "json";
      case KvFormatConfig f -> "kv";
      case OtlpFormatConfig f -> "otlp";
      case ParquetFormatConfig f -> "parquet";
      case ProtobufFormatConfig f -> "protobuf";
      case TextFormatConfig f -> "text";
      case XmlFormatConfig f -> "xml";
      case YamlFormatConfig f -> "yaml";
    };
  }

  /** Whether records are multiline text events. */
  public static boolean multiline(FormatConfig format) {
    return format instanceof TextFormatConfig text
        && text.framing() instanceof FramingConfig.Multiline;
  }

  /**
   * The decoding configuration.
   *
   * @throws IllegalArgumentException when {@code decoding} is not set
   */
  public DecodingConfig decodingConfig() {
    if (decoding == null) {
      throw new IllegalArgumentException("decoding is required");
    }
    return decoding;
  }
}
