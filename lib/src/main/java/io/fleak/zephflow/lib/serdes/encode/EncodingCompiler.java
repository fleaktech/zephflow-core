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

import static io.fleak.zephflow.lib.utils.CompressionUtils.gzip;
import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.databrickssink.AvroToDeltaSchemaConverter;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import io.fleak.zephflow.lib.commands.sink.ParquetBlobFileWriter;
import io.fleak.zephflow.lib.serdes.decode.config.AvroFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.CsvFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.KvFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.OtlpFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ProtobufFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.XmlFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.YamlFormatConfig;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import io.fleak.zephflow.lib.serdes.ser.FieldNames;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;

/**
 * Checks an {@link EncodingConfig} against what one sink can write and builds its encoder. Every
 * config error is an {@link IllegalArgumentException} raised here, naming the sink, the format and
 * the option; only per-record failures are left for write time.
 */
public final class EncodingCompiler {

  private static final Set<DecompressionCodec> WRITABLE_CODECS = Set.of(DecompressionCodec.GZIP);

  private static final Pattern FORBIDDEN_KV_DELIMITER_CHARS =
      Pattern.compile("[A-Za-z0-9_.:/=\"\\\\\\r\\n-]");

  private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {};

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
      throw new IllegalArgumentException(capability.sink() + ": encoding format is required");
    }
    return switch (format) {
      case CsvFormatConfig csv -> csv(csv, config, capability);
      case JsonFormatConfig json -> json(json, config, capability);
      case KvFormatConfig kv -> kv(kv, config, capability);
      case ParquetFormatConfig parquet -> parquet(parquet, config, capability);
      case AvroFormatConfig f -> throw unwritable(capability, "avro");
      case OtlpFormatConfig f -> throw unwritable(capability, "otlp");
      case ProtobufFormatConfig f -> throw unwritable(capability, "protobuf");
      case TextFormatConfig f -> throw unwritable(capability, "text");
      case XmlFormatConfig f -> throw unwritable(capability, "xml");
      case YamlFormatConfig f -> throw unwritable(capability, "yaml");
    };
  }

  private static IllegalArgumentException unwritable(WriterCapability capability, String type) {
    return new IllegalArgumentException(
        capability.sink() + ": format " + type + " cannot be written");
  }

  private static void requireFormat(WriterCapability capability, String type) {
    if (!capability.formats().contains(type)) {
      throw new IllegalArgumentException(capability.sink() + " cannot write " + type);
    }
  }

  /** Whether each unit is gzipped, the only codec a writer produces. */
  private static boolean gzipped(List<DecompressionCodec> codecs, WriterCapability capability) {
    String sink = capability.sink();
    if (codecs == null || codecs.isEmpty()) {
      return false;
    }
    if (codecs.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException(sink + ": compression must not contain empty entries");
    }
    if (codecs.contains(DecompressionCodec.AUTO)) {
      throw new IllegalArgumentException(
          sink
              + ": compression auto describes how a reader guesses; name the codec the data"
              + " carries");
    }
    if (codecs.size() > 1) {
      throw new IllegalArgumentException(
          sink + ": compression takes at most one codec when writing");
    }
    DecompressionCodec codec = codecs.getFirst();
    if (!WRITABLE_CODECS.contains(codec) || !capability.compression().contains(codec)) {
      throw new IllegalArgumentException(
          "compression " + codec.configName() + " cannot be written by " + sink);
    }
    return codec == DecompressionCodec.GZIP;
  }

  private static Plan csv(CsvFormatConfig csv, EncodingConfig config, WriterCapability capability) {
    String sink = capability.sink();
    requireFormat(capability, "csv");
    boolean gzip = gzipped(config.compression(), capability);
    if ("".equals(csv.quote())) {
      throw new IllegalArgumentException(
          sink + ": csv with quoting off cannot be written without losing data");
    }
    if (csv.skipLines() != null && csv.skipLines() != 0) {
      throw new IllegalArgumentException(
          sink + ": csv option skipLines cannot be used when writing");
    }
    if (csv.commentMarker() != null && !csv.commentMarker().isEmpty()) {
      throw new IllegalArgumentException(
          sink + ": csv option commentMarker cannot be used when writing");
    }
    if (csv.charset() != null && !isUtf8(csv.charset())) {
      throw new IllegalArgumentException(
          sink
              + ": csv option charset "
              + csv.charset()
              + " cannot be used when writing; csv is written as UTF-8");
    }
    char delimiter = csvChar(sink, "delimiter", csv.delimiter() == null ? "," : csv.delimiter());
    char quote = csvChar(sink, "quote", csv.quote() == null ? "\"" : csv.quote());
    Character escape =
        csv.escape() == null || csv.escape().isEmpty()
            ? null
            : csvChar(sink, "escape", csv.escape());
    if (delimiter == quote || (escape != null && (escape == delimiter || escape == quote))) {
      throw new IllegalArgumentException(
          sink + ": csv options delimiter, quote and escape must be different characters");
    }
    List<String> columns = csv.columns() == null ? null : csvColumns(sink, csv.columns());
    boolean header = csv.header() == null || csv.header();
    if (!header && columns == null) {
      throw new IllegalArgumentException(
          sink + ": csv option columns must be set when header is false");
    }
    if (capability.csvHeaderOffWithColumns() && header) {
      throw new IllegalArgumentException(
          sink + " writes csv only with header false and columns set");
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

  /** One ASCII character other than a line ending, as the decoder requires. */
  private static char csvChar(String sink, String option, String value) {
    char c = value.length() == 1 ? value.charAt(0) : 0;
    if (value.length() != 1 || c >= 128 || c == '\n' || c == '\r') {
      throw new IllegalArgumentException(
          sink
              + ": csv option "
              + option
              + " must be a single ASCII character other than a line ending");
    }
    return c;
  }

  /** Normalized as the legacy csv serializer does, so the same list writes the same bytes. */
  private static List<String> csvColumns(String sink, List<String> columns) {
    if (columns.isEmpty()) {
      throw new IllegalArgumentException(sink + ": csv option columns must not be empty");
    }
    Set<String> names = new LinkedHashSet<>();
    for (String column : columns) {
      String name = FieldNames.normalize(column);
      if (name.isEmpty()) {
        throw new IllegalArgumentException(sink + ": csv option columns must not hold blank names");
      }
      if (!names.add(name)) {
        throw new IllegalArgumentException(sink + ": csv option columns holds " + name + " twice");
      }
    }
    return List.copyOf(names);
  }

  private static Plan json(
      JsonFormatConfig json, EncodingConfig config, WriterCapability capability) {
    String sink = capability.sink();
    requireFormat(capability, "json");
    boolean gzip = gzipped(config.compression(), capability);
    JsonFormatConfig.Mode mode = json.mode();
    if (mode == null) {
      throw new IllegalArgumentException(
          sink + ": json format needs a mode: object, array or lines");
    }
    if (mode == JsonFormatConfig.Mode.OBJECT && capability.unit() == Unit.MANY_RECORDS) {
      throw new IllegalArgumentException(
          sink
              + ": json mode object holds exactly one record and "
              + sink
              + " writes many records per unit; use mode array or lines");
    }
    if (!capability.jsonModes().contains(mode)) {
      throw new IllegalArgumentException(sink + " cannot write json mode " + mode.configName());
    }
    return switch (mode) {
      case OBJECT ->
          new Text(records -> OBJECT_MAPPER.writeValueAsBytes(records.getFirst()), "json", gzip);
      case ARRAY -> new Text(EncodingCompiler::jsonArray, "json", gzip);
      case LINES -> new Text(EncodingCompiler::jsonLines, "jsonl", gzip);
    };
  }

  private static byte[] jsonArray(List<RecordFleakData> records) throws JsonProcessingException {
    ArrayFleakData array = new ArrayFleakData();
    array.getArrayPayload().addAll(records);
    return OBJECT_MAPPER.writeValueAsBytes(array);
  }

  private static byte[] jsonLines(List<RecordFleakData> records) throws JsonProcessingException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    for (RecordFleakData record : records) {
      if (bytes.size() > 0) {
        bytes.write('\n');
      }
      bytes.writeBytes(OBJECT_MAPPER.writeValueAsBytes(record));
    }
    return bytes.toByteArray();
  }

  private static Plan kv(KvFormatConfig kv, EncodingConfig config, WriterCapability capability) {
    String sink = capability.sink();
    requireFormat(capability, "kv");
    boolean gzip = gzipped(config.compression(), capability);
    List<String> fields = null;
    if (kv.fields() != null && !kv.fields().isEmpty()) {
      Set<String> names = new HashSet<>();
      for (String field : kv.fields()) {
        String name = FieldNames.normalize(field);
        if (name.isEmpty()) {
          throw new IllegalArgumentException(sink + ": kv option fields must not hold blank names");
        }
        if (!names.add(name)) {
          throw new IllegalArgumentException(sink + ": kv option fields holds " + name + " twice");
        }
      }
      fields = List.copyOf(kv.fields());
    }
    String pairDelimiter =
        kv.pairDelimiter() == null || kv.pairDelimiter().isEmpty() ? " " : kv.pairDelimiter();
    if (FORBIDDEN_KV_DELIMITER_CHARS.matcher(pairDelimiter).find()) {
      throw new IllegalArgumentException(
          sink
              + ": kv option pairDelimiter must not contain letters, digits, '_', '.', ':', '/',"
              + " '-', '=', '\"', '\\' or line breaks");
    }
    return new Text(
        new KvUnitWriter(fields, pairDelimiter, Boolean.TRUE.equals(kv.cleanFields())),
        "raw",
        gzip);
  }

  private static Plan parquet(
      ParquetFormatConfig parquet, EncodingConfig config, WriterCapability capability) {
    String sink = capability.sink();
    requireFormat(capability, "parquet");
    boolean gzip = gzipped(config.compression(), capability);
    if (capability.unit() == Unit.ONE_RECORD) {
      throw new IllegalArgumentException(
          sink
              + ": parquet holds many records per unit and "
              + sink
              + " writes one record per unit");
    }
    if (gzip) {
      throw new IllegalArgumentException(
          sink + ": parquet cannot be combined with compression; its pages carry their own");
    }
    Map<String, Object> avroSchema = avroSchema(sink, parquet.schema());
    try {
      AvroToDeltaSchemaConverter.parse(avroSchema);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          sink + ": parquet option schema is not a valid avro record schema: " + e.getMessage(), e);
    }
    return new Parquet(avroSchema);
  }

  private static Map<String, Object> avroSchema(String sink, JsonNode schema) {
    if (schema == null || schema.isNull()) {
      throw new IllegalArgumentException(sink + ": parquet option schema is required when writing");
    }
    JsonNode node = schema;
    if (schema.isTextual()) {
      try {
        node = OBJECT_MAPPER.readTree(schema.asText());
      } catch (JsonProcessingException e) {
        node = null;
      }
    }
    if (node == null || !node.isObject()) {
      throw new IllegalArgumentException(
          sink
              + ": parquet option schema must be an avro record schema, as a JSON object or a"
              + " string holding one");
    }
    return OBJECT_MAPPER.convertValue(node, MAP);
  }
}
