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
import io.fleak.zephflow.api.SourceCommand.SourceType;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3Backend;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandBoundedTest {

  @BeforeEach
  void registerBackend() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
    FsBackendRegistry.unregister("s3");
    FsBackendRegistry.register(new S3Backend());
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.unregister("s3");
  }

  static List<RecordFleakData> run(Map<String, Object> rawCfg, JobContext jc) throws Exception {
    List<RecordFleakData> emitted = new ArrayList<>();
    boolean[] terminated = {false};
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> r) {
            emitted.addAll(r);
          }

          @Override
          public void terminate() {
            terminated[0] = true;
          }
        };
    FsSourceCommand cmd = new FsSourceCommand("node-1", jc);
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    cmd.execute("test-user", out);
    assertTrue(terminated[0], "execute must call out.terminate()");
    return emitted;
  }

  static Map<String, Object> cfg(Path tmp, String encodingType) {
    return Map.of(
        "backend",
        "file",
        "root",
        tmp.toUri().toString(),
        "fileNameRegex",
        "evt_(?<ts>\\d+)\\.log",
        "encodingType",
        encodingType);
  }

  @Test
  void sourceTypeIsBatch() {
    FsSourceCommand cmd = new FsSourceCommand("n", JobContext.builder().build());
    assertEquals(SourceType.BATCH, cmd.sourceType());
  }

  @Test
  void jsonObjectLine_emitsBareRecordsInTimestampOrder(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_3.log"), "{\"v\":\"c\"}\n{\"v\":\"d\"}");
    Files.writeString(tmp.resolve("evt_1.log"), "{\"v\":\"a\"}");
    Files.writeString(tmp.resolve("evt_2.log"), "{\"v\":\"b\"}");
    Files.writeString(tmp.resolve("ignored.txt"), "{\"v\":\"x\"}");

    List<RecordFleakData> emitted = run(cfg(tmp, "JSON_OBJECT_LINE"), JobContext.builder().build());

    List<Object> vs = emitted.stream().map(r -> r.unwrap().get("v")).toList();
    assertEquals(List.of("a", "b", "c", "d"), vs);
    // bare records: no provenance fields added
    assertFalse(emitted.get(0).unwrap().containsKey("file"));
    assertFalse(emitted.get(0).unwrap().containsKey("line"));
  }
}
