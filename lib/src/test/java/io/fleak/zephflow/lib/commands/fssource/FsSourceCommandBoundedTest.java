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

  @Test
  void emitsLinesFromAllMatchingFilesInTimestampOrder(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_3.log"), "z3a\nz3b");
    Files.writeString(tmp.resolve("evt_1.log"), "z1a\nz1b");
    Files.writeString(tmp.resolve("evt_2.log"), "z2a");
    Files.writeString(tmp.resolve("ignored.txt"), "skip");

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

    FsSourceCommand cmd = new FsSourceCommand("node-1", JobContext.builder().build());
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    cmd.execute("test-user", out);

    List<Object> lines = emitted.stream().map(r -> r.unwrap().get("line")).toList();
    assertEquals(List.of("z1a", "z1b", "z2a", "z3a", "z3b"), lines);
  }

  @Test
  void s3WithConfiguredButUnresolvableCredentialIdThrowsIllegalStateException() {
    Map<String, Object> rawCfg =
        Map.of(
            "backend",
            "s3",
            "root",
            "s3://my-bucket/data/",
            "emission",
            Map.of("type", "LINE", "encoding", "utf-8", "lineBatchSize", 10),
            "mode",
            "BOUNDED",
            "partition",
            Map.of("index", 0, "parallelism", 1),
            "backendConfig",
            Map.of("credentialId", "nonexistent-credential-id", "region", "us-east-1"));

    FsSourceCommand cmd = new FsSourceCommand("node-s3", JobContext.builder().build());
    cmd.parseAndValidateArg(rawCfg);

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () -> cmd.initialize(new MetricClientProvider.NoopMetricClientProvider()));
    assertTrue(
        ex.getMessage().contains("nonexistent-credential-id"),
        "Exception message should name the unresolvable credentialId");
  }
}
