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
package io.fleak.zephflow.lib.serdes.encode;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.at;
import static io.fleak.zephflow.lib.commands.filedecode.SourceDecoding.formatName;
import static io.fleak.zephflow.lib.utils.CompressionUtils.gzip;
import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import io.fleak.zephflow.lib.commands.sink.ParquetBlobFileWriter;
import io.fleak.zephflow.lib.serdes.decode.config.AutoFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.AvroFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.CsvFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfigCompiler;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
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
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;

/**
 * Checks an {@link EncodingConfig} against what one sink can write and builds its encoder. Every
 * config error is an {@link IllegalArgumentException} raised here, naming the sink, the format and
 * the option; only per-record failures are left for write time.
 */
public final class EncodingCompiler {

  private static final Pattern FORBIDDEN_KV_DELIMITER_CHARS =
      Pattern.compile("[A-Za-z0-9_.:/=\"\\\\\\r\\n-]");

  private EncodingCompiler() {}

  /** Reports the first config error, if any, and builds nothing. */
  public static void validate(EncodingConfig config, WriterCapability capability) {
    compile(config, capability);
  }

  /** For a sink that writes one record per unit. */
  public static RecordEncoder recordEncoder(EncodingConfig config, WriterCapability capability) {
    if (capability.unit() != Unit.ONE_RECORD) {
      throw new IllegalArgumentException(
          capability.sink()
              + " writes many records per unit; it needs a file writer, not a record encoder");
    }
    return switch (compile(config, capability)) {
      case Text text ->
          record -> {
            byte[] bytes = text.writer().write(List.of(record));
            return text.gzip() ? gzip(bytes) : bytes;
          };
      case Parquet parquet ->
          throw new IllegalStateException("parquet is refused for one record per unit");
    };
  }

  /** For a sink that writes many records per unit: one file per batch, or several for parquet. */
  public static BlobFileWriter<RecordFleakData> fileWriter(
      EncodingConfig config, WriterCapability capability) {
    if (capability.unit() != Unit.MANY_RECORDS) {
      throw new IllegalArgumentException(
          capability.sink()
              + " writes one record per unit; it needs a record encoder, not a file writer");
    }
    return switch (compile(config, capability)) {
      case Text(UnitWriter writer, String extension, boolean gzip) ->
          new TextFileWriter(writer, extension, gzip);
      case Parquet(Map<String, Object> avroSchema) -> new ParquetBlobFileWriter(avroSchema);
    };
  }

  private sealed interface Plan permits Text, Parquet {}

  private record Text(UnitWriter writer, String extension, boolean gzip) implements Plan {}

  private record Parquet(Map<String, Object> avroSchema) implements Plan {}

  private static Plan compile(EncodingConfig config, WriterCapability capability) {
    FormatConfig format = config.format();
    if (format == null) {
      throw at(
          of("format"),
          new IllegalArgumentException(capability.sink() + ": encoding format is required"),
          "Encoding format is required.");
    }
    return switch (format) {
      case CsvFormatConfig csv -> csv(csv, config, capability);
      case JsonFormatConfig json -> json(json, config, capability);
      case KvFormatConfig kv -> kv(kv, config, capability);
      case ParquetFormatConfig parquet -> parquet(parquet, config, capability);
      case AutoFormatConfig f -> throw unwritable(capability, f);
      case AvroFormatConfig f -> throw unwritable(capability, f);
      case OtlpFormatConfig f -> throw unwritable(capability, f);
      case ProtobufFormatConfig f -> throw unwritable(capability, f);
      case TextFormatConfig text -> text(text, config, capability);
      case XmlFormatConfig f -> throw unwritable(capability, f);
      case YamlFormatConfig f -> throw unwritable(capability, f);
    };
  }

  private static IllegalArgumentException unwritable(
      WriterCapability capability, FormatConfig format) {
    return at(
        of("format", "type"),
        new IllegalArgumentException(
            capability.sink() + ": format " + formatName(format) + " cannot be written"),
        "Format cannot be written by "
            + capability.sink()
            + "; allowed formats: "
            + new TreeSet<>(capability.formats())
            + ".");
  }

  private static void requireFormat(WriterCapability capability, FormatConfig format) {
    String type = formatName(format);
    if (!capability.formats().contains(type)) {
      throw at(
          of("format", "type"),
          new IllegalArgumentException(capability.sink() + " cannot write " + type),
          "Allowed formats for "
              + capability.sink()
              + ": "
              + new TreeSet<>(capability.formats())
              + ".");
    }
  }

  private static boolean gzipped(List<DecompressionCodec> codecs, WriterCapability capability) {
    String sink = capability.sink();
    if (codecs == null || codecs.isEmpty()) {
      return false;
    }
    if (codecs.stream().anyMatch(Objects::isNull)) {
      throw at(
          of("compression").index(codecs.indexOf(null)),
          new IllegalArgumentException(sink + ": compression must not contain empty entries"),
          "Compression entries must not be null.");
    }
    if (codecs.contains(DecompressionCodec.AUTO)) {
      throw at(
          of("compression").index(codecs.indexOf(DecompressionCodec.AUTO)),
          new IllegalArgumentException(
              sink
                  + ": compression auto describes how a reader guesses; name the codec the data"
                  + " carries"),
          "Compression auto is only available for reading. " + compressionConstraint(capability));
    }
    if (codecs.size() > 1) {
      throw at(
          of("compression"),
          new IllegalArgumentException(sink + ": compression takes at most one codec when writing"),
          "Encoding compression accepts at most one codec.");
    }
    DecompressionCodec codec = codecs.getFirst();
    if (codec != DecompressionCodec.GZIP || !capability.compression().contains(codec)) {
      throw at(
          of("compression").index(0),
          new IllegalArgumentException(
              "compression " + codec.configName() + " cannot be written by " + sink),
          compressionConstraint(capability));
    }
    return true;
  }

  private static String compressionConstraint(WriterCapability capability) {
    return capability.compression().contains(DecompressionCodec.GZIP)
        ? "Allowed compression for " + capability.sink() + ": gzip, or omit compression."
        : "Compression must be omitted or empty for " + capability.sink() + ".";
  }

  private static Plan csv(CsvFormatConfig csv, EncodingConfig config, WriterCapability capability) {
    String sink = capability.sink();
    requireFormat(capability, csv);
    boolean gzip = gzipped(config.compression(), capability);
    if ("".equals(csv.quote())) {
      throw at(
          of("format", "quote"),
          new IllegalArgumentException(
              sink + ": csv with quoting off cannot be written without losing data"),
          "CSV quote must not be empty when writing; quoting cannot be disabled.");
    }
    if (csv.skipLines() != null && csv.skipLines() != 0) {
      throw at(
          of("format", "skipLines"),
          new IllegalArgumentException(sink + ": csv option skipLines cannot be used when writing"),
          "CSV skipLines must be absent or 0 when writing.");
    }
    if (csv.commentMarker() != null && !csv.commentMarker().isEmpty()) {
      throw at(
          of("format", "commentMarker"),
          new IllegalArgumentException(
              sink + ": csv option commentMarker cannot be used when writing"),
          "CSV commentMarker must be absent or empty when writing.");
    }
    if (csv.charset() != null && !isUtf8(csv.charset())) {
      throw at(
          of("format", "charset"),
          new IllegalArgumentException(
              sink
                  + ": csv option charset "
                  + csv.charset()
                  + " cannot be used when writing; csv is written as UTF-8"),
          "CSV charset must name UTF-8 when writing, or be omitted.");
    }
    char delimiter = csvChar(sink, "delimiter", csv.delimiter() == null ? "," : csv.delimiter());
    char quote = csvChar(sink, "quote", csv.quote() == null ? "\"" : csv.quote());
    Character escape =
        csv.escape() == null || csv.escape().isEmpty()
            ? null
            : csvChar(sink, "escape", csv.escape());
    if (delimiter == quote || (escape != null && (escape == delimiter || escape == quote))) {
      List<ConfigurationIssue> issues = new ArrayList<>();
      String message = "CSV delimiter, quote and escape must be different characters when present.";
      if (delimiter == quote || (escape != null && escape == delimiter))
        issues.add(new ConfigurationIssue(of("format", "delimiter"), message));
      if (delimiter == quote || (escape != null && escape == quote))
        issues.add(new ConfigurationIssue(of("format", "quote"), message));
      if (escape != null && (escape == delimiter || escape == quote))
        issues.add(new ConfigurationIssue(of("format", "escape"), message));
      throw new ConfigurationValidationException(
          new IllegalArgumentException(
              sink + ": csv options delimiter, quote and escape must be different characters"),
          issues);
    }
    List<String> columns = csv.columns() == null ? null : csvColumns(sink, csv.columns());
    boolean header = csv.header() == null || csv.header();
    if (!header && columns == null) {
      throw at(
          of("format", "columns"),
          new IllegalArgumentException(
              sink + ": csv option columns must be set when header is false"),
          "CSV columns are required when header is false.");
    }
    if (capability.csvHeaderOffWithColumns() && header) {
      throw at(
          of("format", "header"),
          new IllegalArgumentException(sink + " writes csv only with header false and columns set"),
          sink + " requires CSV header false and explicit columns.");
    }
    CSVFormat format =
        CSVFormat.DEFAULT.builder().setDelimiter(delimiter).setQuote(quote).setEscape(escape).get();
    return new Text(new CsvUnitWriter(format, columns, header), "csv", gzip);
  }

  private static boolean isUtf8(String charset) {
    try {
      return Charset.forName(charset).equals(StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static char csvChar(String sink, String option, String value) {
    char c = value.length() == 1 ? value.charAt(0) : 0;
    if (value.length() != 1 || c >= 128 || c == '\n' || c == '\r') {
      throw at(
          of("format", option),
          new IllegalArgumentException(
              sink
                  + ": csv option "
                  + option
                  + " must be a single ASCII character other than a line ending"),
          "Expected a single ASCII character other than a line ending.");
    }
    return c;
  }

  private static List<String> csvColumns(String sink, List<String> columns) {
    if (columns.isEmpty()) {
      throw at(
          of("format", "columns"),
          new IllegalArgumentException(sink + ": csv option columns must not be empty"),
          "CSV columns must contain at least one name.");
    }
    return List.copyOf(distinctNames(sink, "csv option columns", columns, of("format", "columns")));
  }

  private static Set<String> distinctNames(
      String sink, String option, List<String> names, ConfigurationPath path) {
    Set<String> normalized = new LinkedHashSet<>();
    for (int index = 0; index < names.size(); index++) {
      String name = names.get(index);
      String value = FieldNames.normalize(name);
      if (value.isEmpty()) {
        throw at(
            path.index(index),
            new IllegalArgumentException(sink + ": " + option + " must not hold blank names"),
            "Field name must not be blank after trimming whitespace and removing Unicode format"
                + " characters.");
      }
      if (!normalized.add(value)) {
        throw at(
            path.index(index),
            new IllegalArgumentException(sink + ": " + option + " holds " + value + " twice"),
            "Field names must be unique after trimming whitespace and removing Unicode format"
                + " characters.");
      }
    }
    return normalized;
  }

  private static Plan json(
      JsonFormatConfig json, EncodingConfig config, WriterCapability capability) {
    String sink = capability.sink();
    requireFormat(capability, json);
    boolean gzip = gzipped(config.compression(), capability);
    JsonFormatConfig.Mode mode = json.mode();
    if (mode == null) {
      throw at(
          of("format", "mode"),
          new IllegalArgumentException(sink + ": json format needs a mode: object, array or lines"),
          "JSON mode is required. " + jsonModeConstraint(capability));
    }
    if (mode == JsonFormatConfig.Mode.OBJECT && capability.unit() == Unit.MANY_RECORDS) {
      throw at(
          of("format", "mode"),
          new IllegalArgumentException(
              sink
                  + ": json mode object holds exactly one record and "
                  + sink
                  + " writes many records per unit; use mode array or lines"),
          "JSON object holds one record, but this sink writes many records per unit. "
              + jsonModeConstraint(capability));
    }
    if (!capability.jsonModes().contains(mode)) {
      throw at(
          of("format", "mode"),
          new IllegalArgumentException(sink + " cannot write json mode " + mode.configName()),
          jsonModeConstraint(capability));
    }
    return switch (mode) {
      case OBJECT ->
          new Text(records -> OBJECT_MAPPER.writeValueAsBytes(records.getFirst()), "json", gzip);
      case ARRAY -> new Text(EncodingCompiler::jsonArray, "json", gzip);
      case LINES -> new Text(EncodingCompiler::jsonLines, "jsonl", gzip);
    };
  }

  private static String jsonModeConstraint(WriterCapability capability) {
    return "Allowed JSON modes for "
        + capability.sink()
        + ": "
        + capability.jsonModes().stream()
            .filter(
                mode ->
                    capability.unit() != Unit.MANY_RECORDS || mode != JsonFormatConfig.Mode.OBJECT)
            .map(JsonFormatConfig.Mode::configName)
            .sorted()
            .toList()
        + ".";
  }

  private static byte[] jsonArray(List<RecordFleakData> records) throws JsonProcessingException {
    ArrayFleakData array = new ArrayFleakData();
    array.getArrayPayload().addAll(records);
    return OBJECT_MAPPER.writeValueAsBytes(array);
  }

  private static byte[] jsonLines(List<RecordFleakData> records) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    for (RecordFleakData record : records) {
      if (bytes.size() > 0) {
        bytes.write('\n');
      }
      OBJECT_MAPPER.writeValue(bytes, record);
    }
    return bytes.toByteArray();
  }

  private static Plan kv(KvFormatConfig kv, EncodingConfig config, WriterCapability capability) {
    String sink = capability.sink();
    requireFormat(capability, kv);
    boolean gzip = gzipped(config.compression(), capability);
    List<String> fields = null;
    if (kv.fields() != null && !kv.fields().isEmpty()) {
      distinctNames(sink, "kv option fields", kv.fields(), of("format", "fields"));
      fields = List.copyOf(kv.fields());
    }
    String pairDelimiter =
        kv.pairDelimiter() == null || kv.pairDelimiter().isEmpty() ? " " : kv.pairDelimiter();
    if (FORBIDDEN_KV_DELIMITER_CHARS.matcher(pairDelimiter).find()) {
      throw at(
          of("format", "pairDelimiter"),
          new IllegalArgumentException(
              sink
                  + ": kv option pairDelimiter must not contain letters, digits, '_', '.', ':',"
                  + " '/', '-', '=', '\"', '\\' or line breaks"),
          "KV pairDelimiter must not contain ASCII letters, digits, '_', '.', ':', '/', '-', '=',"
              + " '\"', '\\' or line breaks.");
    }
    return new Text(
        new KvUnitWriter(fields, pairDelimiter, Boolean.TRUE.equals(kv.cleanFields())),
        "raw",
        gzip);
  }

  private static Plan text(
      TextFormatConfig text, EncodingConfig config, WriterCapability capability) {
    String sink = capability.sink();
    requireFormat(capability, text);
    boolean gzip = gzipped(config.compression(), capability);
    if (text.charset() != null && !isUtf8(text.charset())) {
      throw at(
          of("format", "charset"),
          new IllegalArgumentException(
              sink
                  + ": text option charset "
                  + text.charset()
                  + " cannot be used when writing; text is written as UTF-8"),
          "Text charset must name UTF-8 when writing, or be omitted.");
    }
    try {
      DecodingConfigCompiler.compile(new DecodingConfig(null, text));
    } catch (IllegalArgumentException e) {
      if (e instanceof ConfigurationValidationException diagnostic) {
        throw new ConfigurationValidationException(
            new IllegalArgumentException(
                sink + ": " + e.getMessage(), diagnostic.originalFailure()),
            diagnostic.issues());
      }
      throw new IllegalArgumentException(sink + ": " + e.getMessage(), e);
    }
    boolean manyRecords = capability.unit() == Unit.MANY_RECORDS;
    if (manyRecords && text.framing() instanceof FramingConfig.Whole) {
      throw at(
          of("format", "framing", "type"),
          new IllegalArgumentException(
              sink
                  + ": text framing whole holds one record and "
                  + sink
                  + " writes many records per unit; use framing line or multiline"),
          "This sink writes many records per unit; use text framing line or multiline.");
    }
    return new Text(new TextUnitWriter(manyRecords), "txt", gzip);
  }

  private static Plan parquet(
      ParquetFormatConfig parquet, EncodingConfig config, WriterCapability capability) {
    String sink = capability.sink();
    requireFormat(capability, parquet);
    boolean gzip = gzipped(config.compression(), capability);
    if (capability.unit() == Unit.ONE_RECORD) {
      throw at(
          of("format", "type"),
          new IllegalArgumentException(
              sink
                  + ": parquet holds many records per unit and "
                  + sink
                  + " writes one record per unit"),
          "Parquet requires a sink that writes many records per unit.");
    }
    if (gzip) {
      throw at(
          of("compression"),
          new IllegalArgumentException(
              sink + ": parquet cannot be combined with compression; its pages carry their own"),
          "Parquet cannot be combined with outer compression; its pages carry their own"
              + " compression.");
    }
    return new Parquet(ParquetSchema.parse(sink, parquet.schema()));
  }
}
