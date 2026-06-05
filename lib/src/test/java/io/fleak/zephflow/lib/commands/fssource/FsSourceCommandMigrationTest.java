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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandMigrationTest {

  @BeforeEach
  void reg() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
  }

  @Test
  void noReEmissionOnRestart(@TempDir Path tmp) throws Exception {
    for (int i = 1; i <= 5; i++) Files.writeString(tmp.resolve("evt_" + i + ".log"), "L" + i);
    Map<String, Object> rawCfg =
        Map.of(
            "backend",
            "file",
            "root",
            tmp.toUri().toString(),
            "fileNameRegex",
            "evt_(?<ts>\\d+)\\.log",
            "emission",
            Map.of("type", "LINE", "encoding", "utf-8", "lineBatchSize", 10),
            "mode",
            "BOUNDED",
            "partition",
            Map.of("index", 0, "parallelism", 1));

    List<RecordFleakData> firstRun = run(rawCfg);
    assertEquals(5, firstRun.size());

    List<RecordFleakData> secondRun = run(rawCfg);
    assertEquals(0, secondRun.size(), "should not re-emit on restart");
  }

  private List<RecordFleakData> run(Map<String, Object> rawCfg) throws Exception {
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
}
