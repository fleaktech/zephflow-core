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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import io.fleak.zephflow.lib.serdes.decode.RecordDecoder;
import io.fleak.zephflow.lib.serdes.decode.avro.AvroDatumFormat;
import io.fleak.zephflow.lib.serdes.decode.avro.AvroOcfFormat;
import io.fleak.zephflow.lib.serdes.decode.csv.CsvDialect;
import io.fleak.zephflow.lib.serdes.decode.csv.CsvFormat;
import io.fleak.zephflow.lib.serdes.decode.internal.CompiledDecoder;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatFactory;
import io.fleak.zephflow.lib.serdes.decode.json.JsonArrayFormat;
import io.fleak.zephflow.lib.serdes.decode.json.JsonLinesFormat;
import io.fleak.zephflow.lib.serdes.decode.json.JsonObjectFormat;
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
      case AvroFormatConfig avro -> avro(avro);
      case CsvFormatConfig csv -> csv(csv);
      case JsonFormatConfig json -> json(json);
      case ProtobufFormatConfig protobuf -> protobuf(protobuf);
      case TextFormatConfig text -> text(text);
      case XmlFormatConfig xml -> xml(xml);
      case YamlFormatConfig yaml -> yaml(yaml);
    };
  }

  private static FormatFactory avro(AvroFormatConfig avro) {
    if (avro.container() == null) {
      throw new IllegalArgumentException("avro format needs a container: ocf or datum");
    }
    return switch (avro.container()) {
      case OCF -> {
        if (avro.schema() != null) {
          throw new IllegalArgumentException(
              "an avro object container file carries its own schema");
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
      throw new IllegalArgumentException("avro datum mode needs a schema");
    }
    Schema schema;
    try {
      schema = new Schema.Parser().parse(json.isTextual() ? json.asText() : json.toString());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("invalid avro schema: " + e.getMessage(), e);
    }
    if (schema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException("avro schema must be a record, not " + schema.getType());
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

  private static FormatFactory protobuf(ProtobufFormatConfig protobuf) {
    if (protobuf.framing() == null) {
      throw new IllegalArgumentException(
          "protobuf format needs a framing: single or varint_delimited");
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
      throw new IllegalArgumentException("protobuf format needs a descriptorSet and a messageType");
    }
    FileDescriptorSet set;
    try {
      set = FileDescriptorSet.parseFrom(Base64.getMimeDecoder().decode(descriptorSet));
    } catch (IllegalArgumentException | InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "protobuf descriptorSet is not a base64 FileDescriptorSet: " + e.getMessage(), e);
    }
    Map<String, FileDescriptorProto> protos = new HashMap<>();
    for (FileDescriptorProto file : set.getFileList()) {
      FileDescriptorProto earlier = protos.put(file.getName(), file);
      if (earlier != null && !earlier.equals(file)) {
        throw new IllegalArgumentException(
            "protobuf descriptorSet has two different files named " + file.getName());
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
      throw new IllegalArgumentException(
          "protobuf message type " + messageType + " is not in the descriptorSet");
    }
    if (found.size() > 1) {
      throw new IllegalArgumentException(
          "protobuf message type " + messageType + " is defined in more than one file");
    }
    if (found.get(0).getOptions().getMapEntry()) {
      throw new IllegalArgumentException(
          "protobuf message type " + messageType + " is a map entry, not a message");
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
      throw new IllegalArgumentException(
          "protobuf descriptorSet has a dependency cycle at " + file.getName());
    }
    List<FileDescriptor> dependencies = new ArrayList<>();
    for (String dependency : file.getDependencyList()) {
      FileDescriptorProto proto = protos.get(dependency);
      if (proto == null) {
        throw new IllegalArgumentException(
            "protobuf descriptorSet is missing " + dependency + ", needed by " + file.getName());
      }
      dependencies.add(build(proto, protos, built, visiting));
    }
    try {
      FileDescriptor descriptor =
          FileDescriptor.buildFrom(file, dependencies.toArray(new FileDescriptor[0]));
      built.put(file.getName(), descriptor);
      return descriptor;
    } catch (DescriptorValidationException e) {
      throw new IllegalArgumentException("protobuf descriptorSet is invalid: " + e.getMessage(), e);
    }
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
