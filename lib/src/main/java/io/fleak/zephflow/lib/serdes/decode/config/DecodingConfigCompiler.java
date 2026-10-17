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
import io.fleak.zephflow.lib.serdes.decode.csv.CsvDialect;
import io.fleak.zephflow.lib.serdes.decode.csv.CsvFormat;
import io.fleak.zephflow.lib.serdes.decode.internal.CompiledDecoder;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatFactory;
import io.fleak.zephflow.lib.serdes.decode.json.JsonArrayFormat;
import io.fleak.zephflow.lib.serdes.decode.json.JsonLinesFormat;
import io.fleak.zephflow.lib.serdes.decode.json.JsonObjectFormat;
import io.fleak.zephflow.lib.serdes.decode.text.MultilineRule;
import io.fleak.zephflow.lib.serdes.decode.text.TextLineFormat;
import io.fleak.zephflow.lib.serdes.decode.text.TextMultilineFormat;
import io.fleak.zephflow.lib.serdes.decode.text.TextWholeFormat;
import io.fleak.zephflow.lib.serdes.decode.xml.XmlDocumentFormat;
import io.fleak.zephflow.lib.serdes.decode.xml.XmlElementsFormat;
import io.fleak.zephflow.lib.serdes.decode.yaml.YamlDocumentFormat;
import io.fleak.zephflow.lib.serdes.decode.yaml.YamlMultiDocumentFormat;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.IntStream;

/** Validates a config and builds its decoder. Every config error is reported here, not on read. */
public final class DecodingConfigCompiler {

  /** Absolute; each step is one XML element name, without namespace prefix, wildcard or index. */
  private static final Pattern RECORD_PATH = Pattern.compile("(/[\\p{L}_][\\p{L}\\p{N}_.\\-]*)+");

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
      case CsvFormatConfig csv -> csv(csv);
      case JsonFormatConfig json -> json(json);
      case TextFormatConfig text -> text(text);
      case XmlFormatConfig xml -> xml(xml);
      case YamlFormatConfig yaml -> yaml(yaml);
    };
  }

  private static FormatFactory csv(CsvFormatConfig csv) {
    int delimiter = csvChar("delimiter", csv.delimiter() == null ? "," : csv.delimiter(), false);
    int quote = csvChar("quote", csv.quote() == null ? "\"" : csv.quote(), true);
    int escape = csvChar("escape", csv.escape(), true);
    int comment = csvChar("commentMarker", csv.commentMarker(), true);
    List<Integer> set =
        IntStream.of(delimiter, quote, escape, comment).filter(c -> c >= 0).boxed().toList();
    if (Set.copyOf(set).size() != set.size()) {
      throw new IllegalArgumentException(
          "csv delimiter, quote, escape and commentMarker must be different characters");
    }
    int skipLines = csv.skipLines() == null ? 0 : csv.skipLines();
    if (skipLines < 0) {
      throw new IllegalArgumentException("csv skipLines must not be negative");
    }
    List<String> columns = csv.columns() == null ? null : columns(csv.columns());
    CsvDialect dialect =
        new CsvDialect(
            delimiter,
            quote,
            escape,
            comment,
            Boolean.TRUE.equals(csv.ignoreSurroundingSpaces()),
            skipLines,
            csv.header() == null || csv.header(),
            columns,
            charset(csv.charset()));
    return (in, limits, flags) -> new CsvFormat(in, limits, dialect);
  }

  /**
   * One ASCII character other than a line ending; -1 when absent, or empty and {@code optional}.
   */
  private static int csvChar(String option, String value, boolean optional) {
    if (value == null || (optional && value.isEmpty())) {
      return -1;
    }
    char c = value.length() == 1 ? value.charAt(0) : 0;
    if (value.length() != 1 || c >= 128 || c == '\n' || c == '\r') {
      throw new IllegalArgumentException(
          "csv " + option + " must be a single ASCII character other than a line ending");
    }
    return c;
  }

  private static List<String> columns(List<String> columns) {
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("csv columns must not be empty");
    }
    Set<String> seen = new HashSet<>();
    for (String column : columns) {
      if (column == null || column.isEmpty()) {
        throw new IllegalArgumentException("csv column names must not be empty");
      }
      if (!seen.add(column)) {
        throw new IllegalArgumentException("csv column names must be unique: " + column);
      }
    }
    return List.copyOf(columns);
  }

  private static FormatFactory xml(XmlFormatConfig xml) {
    if (xml.mode() == null) {
      throw new IllegalArgumentException("xml format needs a mode: document or elements");
    }
    return switch (xml.mode()) {
      case DOCUMENT -> {
        if (xml.recordPath() != null) {
          throw new IllegalArgumentException("xml recordPath only applies to elements mode");
        }
        yield (in, limits, flags) -> new XmlDocumentFormat(in, limits);
      }
      case ELEMENTS -> {
        List<String> path = recordPath(xml.recordPath());
        yield (in, limits, flags) -> new XmlElementsFormat(in, limits, flags, path);
      }
    };
  }

  private static List<String> recordPath(String recordPath) {
    if (recordPath == null || !RECORD_PATH.matcher(recordPath).matches()) {
      throw new IllegalArgumentException(
          "xml elements mode needs a recordPath of element names such as /Events/Event");
    }
    return List.of(recordPath.substring(1).split("/"));
  }

  private static FormatFactory yaml(YamlFormatConfig yaml) {
    if (yaml.mode() == null) {
      throw new IllegalArgumentException("yaml format needs a mode: document or multi_document");
    }
    return switch (yaml.mode()) {
      case DOCUMENT -> (in, limits, flags) -> new YamlDocumentFormat(in, limits);
      case MULTI_DOCUMENT -> YamlMultiDocumentFormat::new;
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
    if (text.framing() == null) {
      throw new IllegalArgumentException("text format needs a framing: line, multiline or whole");
    }
    return switch (text.framing()) {
      case FramingConfig.Line line -> {
        boolean skipBlank = line.skipBlank() == null || line.skipBlank();
        yield (in, limits, flags) -> new TextLineFormat(in, limits, charset, skipBlank);
      }
      case FramingConfig.Multiline multiline -> {
        MultilineRule rule = multilineRule(multiline);
        yield (in, limits, flags) -> new TextMultilineFormat(in, limits, charset, rule);
      }
      case FramingConfig.Whole whole ->
          (in, limits, flags) -> new TextWholeFormat(in, limits, charset);
    };
  }

  private static MultilineRule multilineRule(FramingConfig.Multiline multiline) {
    boolean start = multiline.startPattern() != null;
    if (start == (multiline.continuePattern() != null)) {
      throw new IllegalArgumentException(
          "multiline framing needs exactly one of startPattern and continuePattern");
    }
    int maxLines = multiline.maxLines() == null ? 0 : multiline.maxLines();
    if (multiline.maxLines() != null && maxLines < 1) {
      throw new IllegalArgumentException("multiline maxLines must be at least 1");
    }
    String regex = start ? multiline.startPattern() : multiline.continuePattern();
    try {
      return new MultilineRule(Pattern.compile(regex), start, maxLines);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          "invalid " + (start ? "startPattern" : "continuePattern") + ": " + e.getMessage(), e);
    }
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
