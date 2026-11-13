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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.at;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.auto.AutoFormatCursor;
import io.fleak.zephflow.lib.serdes.decode.avro.AvroDatumFormat;
import io.fleak.zephflow.lib.serdes.decode.avro.AvroOcfFormat;
import io.fleak.zephflow.lib.serdes.decode.csv.CsvDialect;
import io.fleak.zephflow.lib.serdes.decode.csv.CsvFormat;
import io.fleak.zephflow.lib.serdes.decode.internal.CompiledDecoder;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatFactory;
import io.fleak.zephflow.lib.serdes.decode.internal.RandomAccessDecoder;
import io.fleak.zephflow.lib.serdes.decode.internal.UndeclaredCompression;
import io.fleak.zephflow.lib.serdes.decode.json.JsonArrayFormat;
import io.fleak.zephflow.lib.serdes.decode.json.JsonLinesFormat;
import io.fleak.zephflow.lib.serdes.decode.json.JsonObjectFormat;
import io.fleak.zephflow.lib.serdes.decode.otlp.OtlpFormat;
import io.fleak.zephflow.lib.serdes.decode.parquet.ParquetFormat;
import io.fleak.zephflow.lib.serdes.decode.protobuf.ProtobufDelimitedFormat;
import io.fleak.zephflow.lib.serdes.decode.protobuf.ProtobufSingleFormat;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.IntStream;
import org.apache.avro.Schema;

/** Validates a config and builds its decoder. Every config error is reported here, not on read. */
public final class DecodingConfigCompiler {

  /** Absolute; each step is one XML element name, without namespace prefix, wildcard or index. */
  private static final Pattern RECORD_PATH = Pattern.compile("(/[\\p{L}_][\\p{L}\\p{N}_.\\-]*)+");

  private DecodingConfigCompiler() {}

  public static RecordDecoder compile(DecodingConfig config) {
    return compile(config, AutoFormatCursor.Unit.FILE);
  }

  /** Replays only the admitted text prefix after the configured decompression, before parsing. */
  public static RecordDecoder compileCapturedFile(DecodingConfig config, long utf8Length) {
    RecordDecoder decoder = compile(config);
    if (!(decoder instanceof CompiledDecoder streamed)) {
      throw new IllegalArgumentException(
          "A partial text capture cannot use a random-access format");
    }
    return streamed.withCapturedTextPrefix(utf8Length);
  }

  public static RecordDecoder compile(DecodingConfig config, AutoFormatCursor.Unit unit) {
    if (config == null) {
      throw at(
          ConfigurationPath.root(),
          new IllegalArgumentException("decoding config is required"),
          "Decoding configuration is required.");
    }
    List<DecompressionCodec> codecs = compression(config.compression());
    FormatConfig format = config.format();
    if (format == null) {
      throw at(
          of("format"),
          new IllegalArgumentException("decoding format is required"),
          "Decoding format is required.");
    }
    return switch (format) {
      case AutoFormatConfig auto ->
          streamed(codecs, (in, limits, flags) -> new AutoFormatCursor(in, limits, flags, unit));
      case AvroFormatConfig avro -> streamed(codecs, avro(avro));
      case CsvFormatConfig csv -> characters(codecs, csv(csv));
      case JsonFormatConfig json -> streamed(codecs, json(json));
      case KvFormatConfig kv ->
          throw at(
              of("format", "type"),
              new IllegalArgumentException("kv cannot be read: no source decodes kv"),
              "The kv format cannot be read; no source decodes kv.");
      case OtlpFormatConfig otlp -> streamed(codecs, otlp(otlp));
      case ParquetFormatConfig parquet -> parquet(codecs);
      case ProtobufFormatConfig protobuf -> streamed(codecs, protobuf(protobuf));
      case TextFormatConfig text -> characters(codecs, text(text));
      case XmlFormatConfig xml -> streamed(codecs, xml(xml));
      case YamlFormatConfig yaml -> streamed(codecs, yaml(yaml));
    };
  }

  private static RecordDecoder streamed(List<DecompressionCodec> codecs, FormatFactory format) {
    return new CompiledDecoder(codecs, format);
  }

  private static RecordDecoder characters(List<DecompressionCodec> codecs, FormatFactory format) {
    return streamed(codecs, codecs.isEmpty() ? UndeclaredCompression.refusing(format) : format);
  }

  private static RecordDecoder parquet(List<DecompressionCodec> codecs) {
    if (!codecs.isEmpty()) {
      throw at(
          of("compression"),
          new IllegalArgumentException(
              "parquet cannot be combined with compression; its pages carry their own"),
          "Parquet cannot be combined with outer compression; its pages carry their own compression.");
    }
    return new RandomAccessDecoder("parquet", ParquetFormat::new);
  }

  private static List<DecompressionCodec> compression(List<DecompressionCodec> codecs) {
    if (codecs == null) {
      return List.of();
    }
    if (codecs.stream().anyMatch(Objects::isNull)) {
      throw at(
          of("compression").index(codecs.indexOf(null)),
          new IllegalArgumentException("compression must not contain empty entries"),
          "Compression entries must not be null.");
    }
    if (codecs.contains(DecompressionCodec.AUTO) && codecs.size() > 1) {
      throw at(
          of("compression"),
          new IllegalArgumentException("compression \"auto\" cannot be combined with others"),
          "Compression auto must be the only entry.");
    }
    return List.copyOf(codecs);
  }

  private static FormatFactory avro(AvroFormatConfig avro) {
    if (avro.container() == null) {
      throw at(
          of("format", "container"),
          new IllegalArgumentException("avro format needs a container: ocf or datum"),
          "Avro container is required; allowed values: ocf, datum.");
    }
    return switch (avro.container()) {
      case OCF -> {
        if (avro.schema() != null) {
          throw at(
              of("format", "schema"),
              new IllegalArgumentException("an avro object container file carries its own schema"),
              "Schema must be absent for the ocf container, which carries its own schema.");
        }
        yield AvroOcfFormat::new;
      }
      case DATUM -> {
        Schema schema = avroSchema(avro.schema());
        yield (in, limits, flags) -> new AvroDatumFormat(in, limits, schema);
      }
    };
  }

  private static Schema avroSchema(JsonNode json) {
    if (json == null || json.isNull()) {
      throw at(
          of("format", "schema"),
          new IllegalArgumentException("avro datum mode needs a schema"),
          "An Avro record schema is required for datum mode.");
    }
    Schema schema;
    try {
      schema = new Schema.Parser().parse(json.isTextual() ? json.asText() : json.toString());
    } catch (RuntimeException e) {
      throw at(
          of("format", "schema"),
          new IllegalArgumentException("invalid avro schema: " + e.getMessage(), e),
          "Invalid Avro schema; expected an Avro record schema as a JSON object or a string containing one.");
    }
    if (schema.getType() != Schema.Type.RECORD) {
      throw at(
          of("format", "schema"),
          new IllegalArgumentException("avro schema must be a record, not " + schema.getType()),
          "Avro schema must have type record.");
    }
    return schema;
  }

  private static FormatFactory csv(CsvFormatConfig csv) {
    int delimiter = csvChar("delimiter", csv.delimiter() == null ? "," : csv.delimiter(), false);
    int quote = csvChar("quote", csv.quote() == null ? "\"" : csv.quote(), true);
    int escape = csvChar("escape", csv.escape(), true);
    int comment = csvChar("commentMarker", csv.commentMarker(), true);
    List<Integer> set =
        IntStream.of(delimiter, quote, escape, comment).filter(c -> c >= 0).boxed().toList();
    if (Set.copyOf(set).size() != set.size()) {
      List<String> names = List.of("delimiter", "quote", "escape", "commentMarker");
      List<Integer> values = List.of(delimiter, quote, escape, comment);
      throw new ConfigurationValidationException(
          new IllegalArgumentException(
              "csv delimiter, quote, escape and commentMarker must be different characters"),
          IntStream.range(0, values.size())
              .filter(
                  index ->
                      values.get(index) >= 0
                          && set.stream().filter(values.get(index)::equals).count() > 1)
              .mapToObj(
                  index ->
                      new ConfigurationIssue(
                          of("format", names.get(index)),
                          "CSV delimiter, quote, escape and commentMarker must be different characters when present."))
              .toList());
    }
    int skipLines = csv.skipLines() == null ? 0 : csv.skipLines();
    if (skipLines < 0) {
      throw at(
          of("format", "skipLines"),
          new IllegalArgumentException("csv skipLines must not be negative"),
          "CSV skipLines must be greater than or equal to 0.");
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
      throw at(
          of("format", option),
          new IllegalArgumentException(
              "csv " + option + " must be a single ASCII character other than a line ending"),
          "Expected a single ASCII character other than a line ending"
              + (optional ? "; an empty string also disables this option." : "."));
    }
    return c;
  }

  private static List<String> columns(List<String> columns) {
    if (columns.isEmpty()) {
      throw at(
          of("format", "columns"),
          new IllegalArgumentException("csv columns must not be empty"),
          "CSV columns must contain at least one name.");
    }
    Set<String> seen = new HashSet<>();
    for (int index = 0; index < columns.size(); index++) {
      String column = columns.get(index);
      if (column == null || column.isEmpty()) {
        throw at(
            of("format", "columns").index(index),
            new IllegalArgumentException("csv column names must not be empty"),
            "Column name must be non-null and non-empty.");
      }
      if (!seen.add(column)) {
        throw at(
            of("format", "columns").index(index),
            new IllegalArgumentException("csv column names must be unique: " + column),
            "Column names must be unique; this name duplicates an earlier entry.");
      }
    }
    return List.copyOf(columns);
  }

  private static FormatFactory xml(XmlFormatConfig xml) {
    if (xml.mode() == null) {
      throw at(
          of("format", "mode"),
          new IllegalArgumentException("xml format needs a mode: document or elements"),
          "XML mode is required; allowed values: document, elements.");
    }
    return switch (xml.mode()) {
      case DOCUMENT -> {
        if (xml.recordPath() != null) {
          throw at(
              of("format", "recordPath"),
              new IllegalArgumentException("xml recordPath only applies to elements mode"),
              "XML recordPath must be absent in document mode; it only applies to elements mode.");
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
      throw at(
          of("format", "recordPath"),
          new IllegalArgumentException(
              "xml elements mode needs a recordPath of element names such as /Events/Event"),
          "XML elements mode requires an absolute path of element names, such as /Events/Event, without namespace prefixes, wildcards or indices.");
    }
    return List.of(recordPath.substring(1).split("/"));
  }

  private static FormatFactory yaml(YamlFormatConfig yaml) {
    if (yaml.mode() == null) {
      throw at(
          of("format", "mode"),
          new IllegalArgumentException("yaml format needs a mode: document or multi_document"),
          "YAML mode is required; allowed values: document, multi_document.");
    }
    return switch (yaml.mode()) {
      case DOCUMENT -> (in, limits, flags) -> new YamlDocumentFormat(in, limits);
      case MULTI_DOCUMENT -> YamlMultiDocumentFormat::new;
    };
  }

  private static FormatFactory otlp(OtlpFormatConfig otlp) {
    OtlpFormatConfig.Signal signal = otlp.signal();
    if (signal == null) {
      throw at(
          of("format", "signal"),
          new IllegalArgumentException("otlp format needs a signal: logs, metrics or traces"),
          "OTLP signal is required; allowed values: logs, metrics, traces.");
    }
    return (in, limits, flags) -> new OtlpFormat(in, limits, signal);
  }

  private static FormatFactory protobuf(ProtobufFormatConfig protobuf) {
    if (protobuf.framing() == null) {
      throw at(
          of("format", "framing"),
          new IllegalArgumentException(
              "protobuf format needs a framing: single or varint_delimited"),
          "Protobuf framing is required; allowed values: single, varint_delimited.");
    }
    Descriptor type = messageType(protobuf.descriptorSet(), protobuf.messageType());
    return switch (protobuf.framing()) {
      case SINGLE -> (in, limits, flags) -> new ProtobufSingleFormat(in, limits, type);
      case VARINT_DELIMITED -> (in, limits, flags) -> new ProtobufDelimitedFormat(in, limits, type);
    };
  }

  /** Builds every file of the set, each after its dependencies, and finds the message. */
  private static Descriptor messageType(String descriptorSet, String messageType) {
    if (descriptorSet == null || messageType == null) {
      List<ConfigurationIssue> issues = new ArrayList<>();
      if (descriptorSet == null)
        issues.add(
            new ConfigurationIssue(
                of("format", "descriptorSet"),
                "Protobuf descriptorSet is required as a base64 FileDescriptorSet."));
      if (messageType == null)
        issues.add(
            new ConfigurationIssue(
                of("format", "messageType"),
                "Protobuf messageType is required and must name a message in descriptorSet."));
      throw new ConfigurationValidationException(
          new IllegalArgumentException("protobuf format needs a descriptorSet and a messageType"),
          issues);
    }
    FileDescriptorSet set;
    try {
      set =
          FileDescriptorSet.parseFrom(
              Base64.getDecoder().decode(descriptorSet.replaceAll("\\s", "")));
    } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
      throw at(
          of("format", "descriptorSet"),
          new IllegalArgumentException(
              "protobuf descriptorSet is not a base64 FileDescriptorSet: " + e.getMessage(), e),
          "Expected a base64-encoded Protobuf FileDescriptorSet.");
    }
    Map<String, FileDescriptorProto> protos = new HashMap<>();
    for (FileDescriptorProto file : set.getFileList()) {
      FileDescriptorProto earlier = protos.put(file.getName(), file);
      if (earlier != null && !earlier.equals(file)) {
        throw at(
            of("format", "descriptorSet"),
            new IllegalArgumentException(
                "protobuf descriptorSet has two different files named " + file.getName()),
            "Descriptor files sharing a name must have identical definitions.");
      }
    }
    Map<String, FileDescriptor> built = new HashMap<>();
    String wanted = messageType.startsWith(".") ? messageType.substring(1) : messageType;
    List<Descriptor> found = new ArrayList<>();
    for (FileDescriptorProto file : protos.values()) {
      for (Descriptor message : allMessages(build(file, protos, built, new HashSet<>()))) {
        if (message.getFullName().equals(wanted)) {
          found.add(message);
        }
      }
    }
    if (found.isEmpty()) {
      throw at(
          of("format", "messageType"),
          new IllegalArgumentException(
              "protobuf message type " + messageType + " is not in the descriptorSet"),
          "Message type must name a message defined in descriptorSet.");
    }
    if (found.size() > 1) {
      throw at(
          of("format", "messageType"),
          new IllegalArgumentException(
              "protobuf message type " + messageType + " is defined in more than one file"),
          "Message type must be defined in exactly one descriptor file.");
    }
    if (found.get(0).getOptions().getMapEntry()) {
      throw at(
          of("format", "messageType"),
          new IllegalArgumentException(
              "protobuf message type " + messageType + " is a map entry, not a message"),
          "Message type must name a message, not a generated map entry.");
    }
    return found.get(0);
  }

  private static List<Descriptor> allMessages(FileDescriptor file) {
    List<Descriptor> all = new ArrayList<>();
    Deque<Descriptor> pending = new ArrayDeque<>(file.getMessageTypes());
    while (!pending.isEmpty()) {
      Descriptor next = pending.pop();
      all.add(next);
      pending.addAll(next.getNestedTypes());
    }
    return all;
  }

  private static FileDescriptor build(
      FileDescriptorProto file,
      Map<String, FileDescriptorProto> protos,
      Map<String, FileDescriptor> built,
      Set<String> visiting) {
    FileDescriptor done = built.get(file.getName());
    if (done != null) {
      return done;
    }
    if (!visiting.add(file.getName())) {
      throw at(
          of("format", "descriptorSet"),
          new IllegalArgumentException(
              "protobuf descriptorSet has a dependency cycle at " + file.getName()),
          "Descriptor file dependencies must not contain a cycle.");
    }
    List<FileDescriptor> dependencies = new ArrayList<>();
    for (String dependency : file.getDependencyList()) {
      FileDescriptorProto proto = protos.get(dependency);
      if (proto == null) {
        throw at(
            of("format", "descriptorSet"),
            new IllegalArgumentException(
                "protobuf descriptorSet is missing "
                    + dependency
                    + ", needed by "
                    + file.getName()),
            "Descriptor set must include all imported descriptor files.");
      }
      dependencies.add(build(proto, protos, built, visiting));
    }
    try {
      FileDescriptor descriptor =
          FileDescriptor.buildFrom(file, dependencies.toArray(new FileDescriptor[0]));
      built.put(file.getName(), descriptor);
      return descriptor;
    } catch (DescriptorValidationException e) {
      throw at(
          of("format", "descriptorSet"),
          new IllegalArgumentException("protobuf descriptorSet is invalid: " + e.getMessage(), e),
          "Invalid Protobuf descriptor definitions; messages and references must form a valid FileDescriptorSet.");
    }
  }

  private static FormatFactory json(JsonFormatConfig json) {
    if (json.mode() == null) {
      throw at(
          of("format", "mode"),
          new IllegalArgumentException("json format needs a mode: object, array or lines"),
          "JSON mode is required; allowed values: object, array, lines.");
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
      throw at(
          of("format", "framing"),
          new IllegalArgumentException("text format needs a framing: line, multiline or whole"),
          "Text framing is required; allowed types: line, multiline, whole.");
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
      throw new ConfigurationValidationException(
          new IllegalArgumentException(
              "multiline framing needs exactly one of startPattern and continuePattern"),
          List.of("startPattern", "continuePattern").stream()
              .map(
                  field ->
                      new ConfigurationIssue(
                          of("format", "framing", field),
                          "Multiline framing requires exactly one of startPattern and continuePattern."))
              .toList());
    }
    int maxLines = multiline.maxLines() == null ? 0 : multiline.maxLines();
    if (multiline.maxLines() != null && maxLines < 1) {
      throw at(
          of("format", "framing", "maxLines"),
          new IllegalArgumentException("multiline maxLines must be at least 1"),
          "Multiline maxLines must be greater than or equal to 1 when set.");
    }
    String regex = start ? multiline.startPattern() : multiline.continuePattern();
    try {
      return new MultilineRule(Pattern.compile(regex), start, maxLines);
    } catch (PatternSyntaxException e) {
      throw at(
          of("format", "framing", start ? "startPattern" : "continuePattern"),
          new IllegalArgumentException(
              "invalid " + (start ? "startPattern" : "continuePattern") + ": " + e.getMessage(), e),
          "Invalid regular expression: " + e.getDescription() + " at index " + e.getIndex() + ".");
    }
  }

  private static Charset charset(String name) {
    if (name == null) {
      return StandardCharsets.UTF_8;
    }
    try {
      return Charset.forName(name);
    } catch (IllegalArgumentException e) {
      throw at(
          of("format", "charset"),
          new IllegalArgumentException("unknown charset: " + name, e),
          "Unknown charset; use a supported charset name such as UTF-8.");
    }
  }
}
