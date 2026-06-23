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
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandEncodingTest {

  @BeforeEach
  void reg() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
  }

  private List<RecordFleakData> run(Path tmp, String encodingType) throws Exception {
    Map<String, Object> rawCfg =
        Map.of(
            "backend",
            "file",
            "root",
            tmp.toUri().toString(),
            "fileNameRegex",
            "evt_(?<ts>\\d+)\\..*",
            "encodingType",
            encodingType);
    List<RecordFleakData> emitted = new ArrayList<>();
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> r) {
            emitted.addAll(r);
          }

          @Override
          public void terminate() {}
        };
    FsSourceCommand cmd = new FsSourceCommand("n", JobContext.builder().build());
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    cmd.execute("u", out);
    return emitted;
  }

  @Test
  void jsonObject_singleRecord(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_1.json"), "{\"k\":\"v\"}");
    List<RecordFleakData> out = run(tmp, "JSON_OBJECT");
    assertEquals(1, out.size());
    assertEquals("v", out.get(0).unwrap().get("k"));
  }

  @Test
  void jsonArray_fansOut(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_1.json"), "[{\"k\":1},{\"k\":2}]");
    List<RecordFleakData> out = run(tmp, "JSON_ARRAY");
    assertEquals(2, out.size());
  }

  @Test
  void text_emitsRecordPerFile(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_1.txt"), "hello\nworld");
    List<RecordFleakData> out = run(tmp, "STRING_LINE");
    assertEquals(2, out.size());
    assertEquals("hello", out.get(0).unwrap().get("__raw__"));
  }

  @Test
  void csv_parses(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_1.csv"), "a,b\n1,2\n3,4");
    List<RecordFleakData> out = run(tmp, "CSV");
    assertEquals(2, out.size());
    assertEquals("1", String.valueOf(out.get(0).unwrap().get("a")));
  }

  @Test
  void gzip_isAutoDetectedAndDecompressed(@TempDir Path tmp) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
      gz.write("{\"v\":\"a\"}\n{\"v\":\"b\"}".getBytes(StandardCharsets.UTF_8));
    }
    Files.write(tmp.resolve("evt_1.jsonl.gz"), bos.toByteArray());
    List<RecordFleakData> out = run(tmp, "JSON_OBJECT_LINE");
    assertEquals(List.of("a", "b"), out.stream().map(r -> r.unwrap().get("v")).toList());
  }

  @Test
  void nonGzipBytesPassThrough() {
    byte[] plain = "{\"v\":1}".getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(plain, FsSourceCommand.maybeGunzip(plain));
  }

  /**
   * A corrupt file (malformed JSON) must NOT abort the whole scan. The valid file's records must
   * still be emitted and out.terminate() must be called.
   */
  @Test
  void corruptFile_isSkipped_validFileStillEmitted(@TempDir Path tmp) throws Exception {
    // valid file — will be processed
    Files.writeString(tmp.resolve("evt_1.json"), "{\"k\":\"good\"}");
    // corrupt file — malformed JSON causes deserializer to throw
    Files.writeString(tmp.resolve("evt_2.json"), "{not valid");

    List<RecordFleakData> emitted = new ArrayList<>();
    boolean[] terminateCalled = {false};
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> r) {
            emitted.addAll(r);
          }

          @Override
          public void terminate() {
            terminateCalled[0] = true;
          }
        };

    FsSourceCommand cmd = new FsSourceCommand("n", JobContext.builder().build());
    Map<String, Object> rawCfg =
        Map.of(
            "backend",
            "file",
            "root",
            tmp.toUri().toString(),
            "fileNameRegex",
            "evt_(?<ts>\\d+)\\..*",
            "encodingType",
            "JSON_OBJECT");
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());

    // must not throw
    assertDoesNotThrow(() -> cmd.execute("u", out));

    // valid record must be emitted
    assertEquals(1, emitted.size());
    assertEquals("good", emitted.get(0).unwrap().get("k"));

    // terminate must always be called
    assertTrue(terminateCalled[0], "out.terminate() must be called even when a file is corrupt");
  }
}
