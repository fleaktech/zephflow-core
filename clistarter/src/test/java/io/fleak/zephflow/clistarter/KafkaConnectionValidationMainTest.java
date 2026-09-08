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
package io.fleak.zephflow.clistarter;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.utils.JsonUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KafkaConnectionValidationMainTest {
  @TempDir Path directory;

  @Test
  void malformedInputNeverLeaksSecrets() throws Exception {
    Path input = directory.resolve("input.json");
    Path result = directory.resolve("result.json");
    Files.writeString(input, "{\"SENTINEL_PASSWORD\"");
    KafkaConnectionValidationMain.run(
        new String[] {
          input.toString(), result.toString(), Long.toString(System.currentTimeMillis() + 1000)
        });
    assertEquals("{\"status\":\"UNAVAILABLE\",\"checks\":[]}", Files.readString(result));
  }

  @Test
  void nonKafkaCommandCannotExecuteEvenWhenInputAttemptsToIncludeIt() throws Exception {
    var result =
        run(
            List.of(
                Map.of(
                    "id",
                    "side-effect",
                    "commandName",
                    "filesink",
                    "config",
                    Map.of("path", directory.resolve("unexpected").toString()))),
            1000);
    assertEquals("UNSUPPORTED", result.at("/checks/0/status").asText());
    assertFalse(Files.exists(directory.resolve("unexpected")));
  }

  @Test
  void expiredMultipleNodesReportEveryNodeWithoutStartingClient() throws Exception {
    var nodes =
        List.of(
            Map.of("id", "source", "commandName", "kafkasource"),
            Map.of("id", "sink", "commandName", "kafkasink"));
    var result = run(nodes, -1000);
    assertEquals(2, result.get("checks").size());
    assertEquals("UNAVAILABLE", result.at("/checks/0/status").asText());
    assertEquals("UNAVAILABLE", result.at("/checks/1/status").asText());
  }

  @Test
  void duplicateAndEmptyNodesFailClosed() throws Exception {
    assertEquals("UNAVAILABLE", run(List.of(), 1000).get("status").asText());
    var node = Map.of("id", "same", "commandName", "kafkasource");
    assertEquals("UNAVAILABLE", run(List.of(node, node), 1000).get("status").asText());
  }

  @Test
  void invalidConfigReportsOnlySafeCategoryAndExpectedIdentity() throws Exception {
    var result =
        run(
            List.of(
                Map.of(
                    "id",
                    "source",
                    "commandName",
                    "kafkasource",
                    "config",
                    Map.of("broker", "SENTINEL_PASSWORD"))),
            1000);
    assertEquals("INVALID_CONFIGURATION", result.at("/checks/0/status").asText());
    assertFalse(result.toString().contains("SENTINEL_PASSWORD"));
  }

  @Test
  void isolatedSmallHeapProcessWritesSafeResultWithoutStartingDag() throws Exception {
    Path input = directory.resolve("process-input.json");
    Path result = directory.resolve("process-result.json");
    Path output = directory.resolve("process-output.log");
    Files.writeString(
        input,
        "{\"dag\":[{\"id\":\"source\",\"commandName\":\"kafkasource\",\"config\":{\"broker\":\"SENTINEL_PASSWORD\"}}]}");
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xms32m",
                "-Xmx256m",
                "-cp",
                System.getProperty("validation.test.classpath"),
                KafkaConnectionValidationMain.class.getName(),
                input.toString(),
                result.toString(),
                Long.toString(System.currentTimeMillis() + 10000))
            .redirectErrorStream(true)
            .redirectOutput(output.toFile())
            .start();
    try {
      assertTrue(process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS));
      assertEquals(0, process.exitValue());
      assertEquals(
          "INVALID_CONFIGURATION",
          JsonUtils.OBJECT_MAPPER
              .readTree(Files.readString(result))
              .at("/checks/0/status")
              .asText());
      assertFalse(Files.readString(output).contains("SENTINEL_PASSWORD"));
      assertFalse(Files.readString(output).contains("DagExecutor"));
    } finally {
      process.destroyForcibly();
      process.waitFor(2, java.util.concurrent.TimeUnit.SECONDS);
    }
  }

  private com.fasterxml.jackson.databind.JsonNode run(List<?> nodes, long remaining)
      throws Exception {
    Path input = directory.resolve("input.json");
    Path result = directory.resolve("result.json");
    Files.writeString(input, JsonUtils.OBJECT_MAPPER.writeValueAsString(Map.of("dag", nodes)));
    KafkaConnectionValidationMain.run(
        new String[] {
          input.toString(), result.toString(), Long.toString(System.currentTimeMillis() + remaining)
        });
    return JsonUtils.OBJECT_MAPPER.readTree(Files.readString(result));
  }
}
