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
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Tag("slow")
class BoundedMemoryStreamingTest {

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
  void streams2GbFileWithoutOom(@TempDir Path tmp) throws Exception {
    Path file = tmp.resolve("evt_1.log");
    long targetBytes = 2L * 1024 * 1024 * 1024; // 2 GB
    String line = "x".repeat(1000); // ~1000 bytes per line
    long expectedLines = targetBytes / (line.length() + 1);
    try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      for (long i = 0; i < expectedLines; i++) {
        w.write(line);
        w.newLine();
      }
    }
    assertTrue(
        Files.size(file) >= targetBytes - 4096,
        "synthetic input must be ≥ 2 GB, was " + Files.size(file));

    Map<String, Object> rawCfg =
        Map.of(
            "backend",
            "file",
            "root",
            tmp.toUri().toString(),
            "fileNameRegex",
            "evt_(?<ts>\\d+)\\.log",
            "emission",
            Map.of("type", "LINE", "encoding", "utf-8", "lineBatchSize", 500),
            "mode",
            "BOUNDED",
            "partition",
            Map.of("index", 0, "parallelism", 1));

    AtomicLong count = new AtomicLong();
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> r) {
            count.addAndGet(r.size());
          }

          @Override
          public void terminate() {}
        };

    FsSourceCommand cmd = new FsSourceCommand("n", JobContext.builder().build());
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    cmd.execute("u", out);

    assertEquals(expectedLines, count.get());
  }
}
