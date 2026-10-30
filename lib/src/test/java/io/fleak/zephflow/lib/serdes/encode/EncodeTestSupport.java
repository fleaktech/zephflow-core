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

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeResult;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfigCompiler;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig.Mode;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class EncodeTestSupport {

  static final WriterCapability S3 =
      new WriterCapability(
          "s3sink",
          Unit.MANY_RECORDS,
          Set.of("csv", "json", "kv", "parquet"),
          Set.of(Mode.ARRAY, Mode.LINES),
          false,
          Set.of(DecompressionCodec.GZIP));

  static final WriterCapability KAFKA =
      new WriterCapability(
          "kafkasink",
          Unit.ONE_RECORD,
          Set.of("csv", "json", "kv"),
          Set.of(Mode.OBJECT, Mode.ARRAY, Mode.LINES),
          false,
          Set.of());

  static final WriterCapability SPLUNK =
      new WriterCapability(
          "splunkhecsink",
          Unit.ONE_RECORD,
          Set.of("csv", "json", "kv"),
          Set.of(Mode.OBJECT),
          true,
          Set.of());

  static final WriterCapability SQS =
      new WriterCapability(
          "sqssink", Unit.ONE_RECORD, Set.of("json"), Set.of(Mode.OBJECT), false, Set.of());

  private EncodeTestSupport() {}

  static EncodingConfig encoding(String json) {
    try {
      return JsonUtils.OBJECT_MAPPER.readValue(json, EncodingConfig.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static EncodingConfig encoding(String compression, String format) {
    return encoding("{\"compression\":" + compression + ",\"format\":" + format + "}");
  }

  static DecodingConfig decoding(String compression, String format) {
    try {
      return JsonUtils.OBJECT_MAPPER.readValue(
          "{\"compression\":" + compression + ",\"format\":" + format + "}", DecodingConfig.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static DecodeResult decode(DecodingConfig decoding, byte[] bytes) {
    return DecodingConfigCompiler.compile(decoding).decodeAll(bytes, DecodeLimits.UNLIMITED);
  }

  /** Decodes every file and joins the records, keeping the problems of all files. */
  static DecodeResult decodeFiles(DecodingConfig decoding, List<byte[]> files) {
    List<RecordFleakData> records = new ArrayList<>();
    List<DecodeOutcome> problems = new ArrayList<>();
    for (byte[] file : files) {
      DecodeResult result = decode(decoding, file);
      records.addAll(result.records());
      problems.addAll(result.problems());
    }
    return new DecodeResult(records, problems);
  }

  static List<byte[]> writeFiles(
      BlobFileWriter<RecordFleakData> writer, List<RecordFleakData> records, Path dir)
      throws Exception {
    List<byte[]> contents = new ArrayList<>();
    for (File file : writer.writeToTempFiles(records, dir)) {
      contents.add(Files.readAllBytes(file.toPath()));
    }
    return contents;
  }

  static byte[] writeOneFile(
      BlobFileWriter<RecordFleakData> writer, List<RecordFleakData> records, Path dir)
      throws Exception {
    List<byte[]> files = writeFiles(writer, records, dir);
    if (files.size() != 1) {
      throw new AssertionError("expected one file, got " + files.size());
    }
    return files.getFirst();
  }

  static RecordFleakData record(String... keysAndValues) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put(keysAndValues[i], keysAndValues[i + 1]);
    }
    return (RecordFleakData) FleakData.wrap(map);
  }

  static RecordFleakData json(String json) {
    return (RecordFleakData) JsonUtils.loadFleakDataFromJsonString(json);
  }

  /** One char per byte, so bytes compare exactly and ASCII stays readable in a failure. */
  static String latin1(byte[] bytes) {
    return new String(bytes, StandardCharsets.ISO_8859_1);
  }
}
