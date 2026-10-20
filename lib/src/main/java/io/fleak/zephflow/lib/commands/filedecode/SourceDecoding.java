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

import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.config.AvroFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.CsvFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FramingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ProtobufFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.XmlFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.YamlFormatConfig;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import java.util.List;

/**
 * The inputs that decide how a file source's bytes are decoded: either the legacy {@code
 * encodingType} (with an optional {@code compressionType}) or a {@code decoding} configuration.
 */
public record SourceDecoding(
    EncodingType encodingType, CompressionType compressionType, DecodingConfig decoding) {

  /** The format's type name, as {@code decoding} spells it ({@code json}, {@code csv}, ...). */
  public String formatName() {
    return formatName(decodingConfig().format());
  }

  public static String formatName(FormatConfig format) {
    return switch (format) {
      case AvroFormatConfig f -> "avro";
      case CsvFormatConfig f -> "csv";
      case JsonFormatConfig f -> "json";
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
   * The decoding configuration: {@code decoding} as given, or the legacy settings translated.
   *
   * @throws IllegalArgumentException when not exactly one of {@code encodingType} and {@code
   *     decoding} is set, when {@code compressionType} comes with {@code decoding}, or for {@code
   *     PARQUET}
   */
  public DecodingConfig decodingConfig() {
    if ((encodingType == null) == (decoding == null)) {
      throw new IllegalArgumentException("exactly one of encodingType and decoding must be set");
    }
    if (decoding != null) {
      if (compressionType != null) {
        throw new IllegalArgumentException(
            "compressionType is only allowed with encodingType; decoding carries its own compression");
      }
      return decoding;
    }
    DeserializerFactory.validateEncodingType(encodingType);
    DecompressionCodec codec =
        compressionType == CompressionType.GZIP ? DecompressionCodec.GZIP : DecompressionCodec.AUTO;
    return new DecodingConfig(List.of(codec), format(encodingType));
  }

  private static FormatConfig format(EncodingType encodingType) {
    return switch (encodingType) {
      case JSON_OBJECT -> new JsonFormatConfig(JsonFormatConfig.Mode.OBJECT);
      case JSON_ARRAY -> new JsonFormatConfig(JsonFormatConfig.Mode.ARRAY);
      case JSON_OBJECT_LINE -> new JsonFormatConfig(JsonFormatConfig.Mode.LINES);
      case STRING_LINE -> new TextFormatConfig(null, new FramingConfig.Line(false));
      case TEXT -> new TextFormatConfig(null, new FramingConfig.Whole());
      case CSV -> new CsvFormatConfig(null, null, null, null, null, null, null, true, null);
      case XML -> new XmlFormatConfig(XmlFormatConfig.Mode.DOCUMENT, null);
      case PARQUET -> throw new IllegalArgumentException("PARQUET is rejected; use decoding");
      case KV -> throw new IllegalArgumentException("KV is a sink-only format");
    };
  }
}
