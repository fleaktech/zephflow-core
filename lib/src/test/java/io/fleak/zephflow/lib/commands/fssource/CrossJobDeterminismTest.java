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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CrossJobDeterminismTest {

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
  void threeJobsOwnDisjointSlicesUnioningToTotal(@TempDir Path tmp) throws Exception {
    int totalFiles = 300;
    for (int i = 0; i < totalFiles; i++) {
      Files.writeString(tmp.resolve("evt_" + i + ".log"), "content-" + i);
    }

    Set<String> emittedByJob0 = runJob(tmp, 0, 3);
    Set<String> emittedByJob1 = runJob(tmp, 1, 3);
    Set<String> emittedByJob2 = runJob(tmp, 2, 3);

    Set<String> union = new HashSet<>();
    union.addAll(emittedByJob0);
    union.addAll(emittedByJob1);
    union.addAll(emittedByJob2);
    assertEquals(totalFiles, union.size(), "Total emitted across jobs must equal total files");

    Set<String> overlap = new HashSet<>(emittedByJob0);
    overlap.retainAll(emittedByJob1);
    assertTrue(overlap.isEmpty(), "No overlap between jobs 0 and 1");
    overlap = new HashSet<>(emittedByJob0);
    overlap.retainAll(emittedByJob2);
    assertTrue(overlap.isEmpty(), "No overlap between jobs 0 and 2");
    overlap = new HashSet<>(emittedByJob1);
    overlap.retainAll(emittedByJob2);
    assertTrue(overlap.isEmpty(), "No overlap between jobs 1 and 2");
  }

  private Set<String> runJob(Path tmp, int idx, int parallelism) throws Exception {
    Map<String, Object> rawCfg =
        Map.of(
            "backend",
            "file",
            "root",
            tmp.toUri().toString(),
            "fileNameRegex",
            "evt_(?<ts>\\d+)\\.log",
            "emission",
            Map.of("type", "LINE", "encoding", "utf-8", "lineBatchSize", 100),
            "mode",
            "BOUNDED",
            "partition",
            Map.of("index", idx, "parallelism", parallelism));

    Set<String> emitted = new HashSet<>();
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> r) {
            for (RecordFleakData rec : r) emitted.add((String) rec.unwrap().get("file"));
          }

          @Override
          public void terminate() {}
        };

    FsSourceCommand cmd = new FsSourceCommand("n" + idx, JobContext.builder().build());
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    cmd.execute("u", out);
    return emitted;
  }
}
