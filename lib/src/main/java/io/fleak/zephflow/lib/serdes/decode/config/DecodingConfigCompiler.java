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

import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.internal.CompiledDecoder;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatFactory;
import io.fleak.zephflow.lib.serdes.decode.json.JsonArrayFormat;
import io.fleak.zephflow.lib.serdes.decode.json.JsonLinesFormat;
import io.fleak.zephflow.lib.serdes.decode.json.JsonObjectFormat;
import io.fleak.zephflow.lib.serdes.decode.text.TextLineFormat;
import io.fleak.zephflow.lib.serdes.decode.text.TextWholeFormat;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/** Validates a config and builds its decoder. Every config error is reported here, not on read. */
public final class DecodingConfigCompiler {

  private DecodingConfigCompiler() {}

  public static RecordDecoder compile(DecodingConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("decoding config is required");
    }
    return new CompiledDecoder(compression(config.compression()), format(config.format()));
  }

  private static List<DecompressionCodec> compression(List<DecompressionCodec> codecs) {
    if (codecs == null) {
      return List.of();
    }
    if (codecs.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("compression must not contain empty entries");
    }
    if (codecs.contains(DecompressionCodec.AUTO) && codecs.size() > 1) {
      throw new IllegalArgumentException("compression \"auto\" cannot be combined with others");
    }
    return List.copyOf(codecs);
  }

  private static FormatFactory format(FormatConfig format) {
    if (format == null) {
      throw new IllegalArgumentException("decoding format is required");
    }
    return switch (format) {
      case JsonFormatConfig json -> json(json);
      case TextFormatConfig text -> text(text);
    };
  }

  private static FormatFactory json(JsonFormatConfig json) {
    if (json.mode() == null) {
      throw new IllegalArgumentException("json format needs a mode: object, array or lines");
    }
    return switch (json.mode()) {
      case OBJECT -> JsonObjectFormat::new;
      case ARRAY -> JsonArrayFormat::new;
      case LINES -> JsonLinesFormat::new;
    };
  }

  private static FormatFactory text(TextFormatConfig text) {
    Charset charset = charset(text.charset());
    if (!StandardCharsets.UTF_8.equals(charset)) {
      throw new IllegalArgumentException("text charset " + charset + " is not supported yet");
    }
    if (text.framing() == null) {
      throw new IllegalArgumentException("text format needs a framing: line or whole");
    }
    return switch (text.framing()) {
      case FramingConfig.Line line -> {
        boolean skipBlank = line.skipBlank() == null || line.skipBlank();
        yield (in, limits, flags) -> new TextLineFormat(in, limits, skipBlank);
      }
      case FramingConfig.Whole whole -> TextWholeFormat::new;
    };
  }

  private static Charset charset(String name) {
    if (name == null) {
      return StandardCharsets.UTF_8;
    }
    try {
      return Charset.forName(name);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("unknown charset: " + name, e);
    }
  }
}
