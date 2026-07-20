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
package io.fleak.zephflow.lib.commands.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.api.JobContext;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StoreForwardPathsTest {

  private static final Path TMP_BASE =
      Path.of(System.getProperty("java.io.tmpdir"), "zephflow-store-forward");

  private static JobContext jobContext(
      Map<String, Serializable> otherProperties, Map<String, String> metricTags) {
    return JobContext.builder().otherProperties(otherProperties).metricTags(metricTags).build();
  }

  @Test
  void explicitLocalStorePathWins() {
    JobContext jc =
        jobContext(
            Map.of(JobContext.STORE_FORWARD_DIR, "/data/injected", JobContext.REPLICA_INDEX, "2"),
            Map.of("job_id", "9f3a"));

    assertEquals(
        Path.of("/custom/base/9f3a/sink_node/replica-2"),
        StoreForwardPaths.resolve("/custom/base", jc, "sink_node"));
  }

  @Test
  void injectedStoreForwardDirUsedWhenNoLocalStorePath() {
    JobContext jc =
        jobContext(
            Map.of(
                JobContext.STORE_FORWARD_DIR, "/data/worker-1/store-forward",
                JobContext.REPLICA_INDEX, "1"),
            Map.of("job_id", "9f3a"));

    assertEquals(
        Path.of("/data/worker-1/store-forward/9f3a/sink_node/replica-1"),
        StoreForwardPaths.resolve(" ", jc, "sink_node"));
  }

  @Test
  void tmpFallbackWithDefaultsForLocalRuns() {
    JobContext jc = JobContext.builder().build();

    assertEquals(
        TMP_BASE.resolve(Path.of("local", "sink_node", "replica-0")),
        StoreForwardPaths.resolve(null, jc, "sink_node"));
  }

  @Test
  void jobIdIsSanitizedForPathSafety() {
    JobContext jc = jobContext(Map.of(), Map.of("job_id", "job/../etc: 1"));

    assertEquals(
        TMP_BASE.resolve(Path.of("job_.._etc__1", "sink_node", "replica-0")),
        StoreForwardPaths.resolve(null, jc, "sink_node"));
  }

  @Test
  void unparseableReplicaIndexFallsBackToZero() {
    JobContext jc =
        jobContext(Map.of(JobContext.REPLICA_INDEX, "not-a-number"), Map.of("job_id", "9f3a"));

    assertEquals(
        TMP_BASE.resolve(Path.of("9f3a", "sink_node", "replica-0")),
        StoreForwardPaths.resolve(null, jc, "sink_node"));
  }

  @Test
  void nullMapsBehaveAsDefaults() {
    JobContext jc = jobContext(null, null);

    assertEquals(
        TMP_BASE.resolve(Path.of("local", "sink_node", "replica-0")),
        StoreForwardPaths.resolve(null, jc, "sink_node"));
  }
}
