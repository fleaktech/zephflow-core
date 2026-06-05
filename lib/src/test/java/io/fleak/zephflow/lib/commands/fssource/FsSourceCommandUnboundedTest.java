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
import java.util.concurrent.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandUnboundedTest {

  @BeforeEach
  void register() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
  }

  @Test
  void picksUpNewFilesOverTime(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_1.log"), "first");

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
            "UNBOUNDED",
            "listingIntervalMs",
            100,
            "partition",
            Map.of("index", 0, "parallelism", 1));

    List<RecordFleakData> emitted = Collections.synchronizedList(new ArrayList<>());
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
    ExecutorService es = Executors.newSingleThreadExecutor();
    Future<?> f =
        es.submit(
            () -> {
              try {
                cmd.execute("u", out);
              } catch (Exception ignored) {
              }
            });

    Thread.sleep(500);
    assertEquals(1, emitted.size());

    Files.writeString(tmp.resolve("evt_2.log"), "second");
    Thread.sleep(500);
    assertEquals(2, emitted.size());

    cmd.terminate();
    f.get(2, TimeUnit.SECONDS);
    es.shutdownNow();
  }
}
