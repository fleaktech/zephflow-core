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

import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Created by bolei on 3/1/25 */
class MainTest {

  @Test
  public void testMainWritesRunSummaryFile(@TempDir Path tempDir) throws Exception {
    Path summaryFile = tempDir.resolve("run-summary.json");

    System.setProperty(Main.RUN_SUMMARY_FILE_SYS_PROP, summaryFile.toString());
    try {
      runMainWithStdioDag();
    } finally {
      System.clearProperty(Main.RUN_SUMMARY_FILE_SYS_PROP);
    }

    assertTrue(Files.exists(summaryFile), "run summary file should be written");
    Map<String, Object> payload =
        fromJsonString(Files.readString(summaryFile), new TypeReference<Map<String, Object>>() {});
    @SuppressWarnings("unchecked")
    Map<String, Object> counters = (Map<String, Object>) payload.get("counters");
    assertEquals(10, ((Number) counters.get("pipeline_input_event_count")).intValue());
    assertEquals(20, ((Number) counters.get("sink_output_count")).intValue());
    String summary = (String) payload.get("summary");
    assertTrue(summary.contains("input events: 10"), summary);
    assertTrue(summary.contains("output events: 20"), summary);
  }

  @Test
  public void testMain() throws Exception {
    String output = runMainWithStdioDag();

    List<String> lines = output.lines().toList();
    var objects =
        lines.stream()
            .filter(l -> l.startsWith("{\""))
            .map(l -> fromJsonString(l, new TypeReference<Map<String, Object>>() {}))
            .collect(Collectors.toSet());
    //noinspection unchecked
    Set<Map<String, Object>> expected =
        new HashSet<>(
            (List<Map<String, Object>>)
                ((Map<String, Object>)
                        fromJsonResource("/expected_output_stdio.json", new TypeReference<>() {}))
                    .get("d"));
    assertEquals(expected, objects);
  }

  private static String runMainWithStdioDag() throws Exception {
    String dagDefStr = MiscUtils.loadStringFromResource("/test_dag_stdio.yml");
    String dagDefBase64Str = MiscUtils.toBase64String(dagDefStr.getBytes());
    String[] args = {"-d", dagDefBase64Str, "-id", "test_job", "-s", "my_service", "-e", "my_env"};

    List<Map<String, Object>> sourceEvents = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      sourceEvents.add(Map.of("num", i));
    }

    try (InputStream in =
            new ByteArrayInputStream(
                Objects.requireNonNull(toJsonString(sourceEvents)).getBytes());
        ByteArrayOutputStream testOut = new ByteArrayOutputStream();
        PrintStream psOut = new PrintStream(testOut)) {
      System.setIn(in);
      System.setOut(psOut);
      Main.main(args);
      return testOut.toString();
    }
  }
}
