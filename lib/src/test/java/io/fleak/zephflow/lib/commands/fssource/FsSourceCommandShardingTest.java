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

class FsSourceCommandShardingTest {

  @BeforeEach
  void setUp() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void tearDown() {
    FsBackendRegistry.unregister("file");
  }

  private List<String> runReplica(Path tempDir, int replicaIndex, int replicaCount)
      throws Exception {
    JobContext jobContext =
        JobContext.builder()
            .otherProperties(
                new HashMap<>(
                    Map.of(
                        JobContext.REPLICA_INDEX, String.valueOf(replicaIndex),
                        JobContext.REPLICA_COUNT, String.valueOf(replicaCount))))
            .build();
    return run(tempDir, jobContext);
  }

  private List<String> run(Path tempDir, JobContext jobContext) throws Exception {
    Map<String, Object> rawConfig =
        Map.of(
            "backend", "file",
            "root", tempDir.toUri().toString(),
            "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
            "encodingType", "JSON_OBJECT_LINE");
    List<String> emittedValues = new ArrayList<>();
    SourceEventAcceptor eventAcceptor =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> records) {
            records.forEach(record -> emittedValues.add((String) record.unwrap().get("v")));
          }

          @Override
          public void terminate() {}
        };
    FsSourceCommand command = new FsSourceCommand("n", jobContext);
    command.parseAndValidateArg(rawConfig);
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    command.execute("u", eventAcceptor);
    return emittedValues;
  }

  @Test
  void threeReplicasProcessDisjointUnionOfAllFiles(@TempDir Path tempDir) throws Exception {
    Set<String> expectedValues = new HashSet<>();
    for (int fileNumber = 1; fileNumber <= 30; fileNumber++) {
      Files.writeString(
          tempDir.resolve("evt_" + fileNumber + ".log"), "{\"v\":\"" + fileNumber + "\"}");
      expectedValues.add(String.valueOf(fileNumber));
    }

    List<String> replica0 = runReplica(tempDir, 0, 3);
    List<String> replica1 = runReplica(tempDir, 1, 3);
    List<String> replica2 = runReplica(tempDir, 2, 3);

    Set<String> allProcessedValues = new HashSet<>();
    for (List<String> replicaValues : List.of(replica0, replica1, replica2)) {
      for (String value : replicaValues) {
        assertTrue(
            allProcessedValues.add(value), "value processed by more than one replica: " + value);
      }
    }
    assertEquals(expectedValues, allProcessedValues);
    assertFalse(replica0.isEmpty());
    assertFalse(replica1.isEmpty());
    assertFalse(replica2.isEmpty());
  }

  @Test
  void noReplicaKeysOwnsAllFiles(@TempDir Path tempDir) throws Exception {
    Set<String> expectedValues = new HashSet<>();
    for (int fileNumber = 1; fileNumber <= 10; fileNumber++) {
      Files.writeString(
          tempDir.resolve("evt_" + fileNumber + ".log"), "{\"v\":\"" + fileNumber + "\"}");
      expectedValues.add(String.valueOf(fileNumber));
    }
    List<String> emittedValues = run(tempDir, JobContext.builder().build());
    assertEquals(expectedValues, new HashSet<>(emittedValues));
  }
}
