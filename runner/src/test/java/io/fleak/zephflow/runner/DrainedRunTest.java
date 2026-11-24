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
package io.fleak.zephflow.runner;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition.DagNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DrainedRunTest {

  @TempDir Path dir;

  @Test
  void aDrainRequestEndsTheRunAsDrained() throws Exception {
    Path ready = dir.resolve("ready");
    Path sinkFile = dir.resolve("sink.jsonl");
    Path statsFile = dir.resolve("stats.json");
    Path controlFile = dir.resolve("control");
    DagExecutor executor = executor(ready, sinkFile);

    CompletableFuture<Boolean> drained =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return executor.executeDagWithRunFiles(statsFile, controlFile);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (!Files.exists(ready) && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertTrue(Files.exists(ready), "the pipeline never consumed its input");
    assertEquals(true, fromJsonString(Files.readString(statsFile), Map.class).get("supportsDrain"));

    Files.writeString(controlFile, "drain");

    assertTrue(drained.get(30, TimeUnit.SECONDS));
    assertEquals(3, Files.readAllLines(sinkFile).size());
  }

  private static DagExecutor executor(Path ready, Path sinkFile) {
    Map<String, CommandFactory> factories =
        Map.of(
            "src",
            SigtermFlushTest.factory(
                CommandType.SOURCE, id -> new SigtermFlushTest.EndlessSource(id, ready)),
            "sink",
            SigtermFlushTest.factory(
                CommandType.SINK, id -> new SigtermFlushTest.FileSink(id, sinkFile)));
    AdjacencyListDagDefinition dag =
        AdjacencyListDagDefinition.builder()
            .jobContext(JobContext.builder().build())
            .dag(
                List.of(
                    DagNode.builder().id("s").commandName("src").outputs(List.of("k")).build(),
                    DagNode.builder().id("k").commandName("sink").outputs(List.of()).build()))
            .build();
    JobConfig jobConfig =
        JobConfig.builder()
            .dagDefinition(dag)
            .jobId("drain_job")
            .environment("test_env")
            .service("test_service")
            .build();
    return DagExecutor.createDagExecutor(
        jobConfig, factories, new MetricClientProvider.NoopMetricClientProvider());
  }
}
