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
import java.util.*;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Created by bolei on 3/1/25 */
class MainTest {

  /**
   * Demonstrates that when a single event has multiple fields processed by python(), and one field
   * fails, the entire event is dropped (no output). This is because the exception propagates out of
   * the dict() expression and the whole event ends up in failureEvents.
   */
  @Test
  public void testSingleEventWithTwoPythonFields_oneInvalidFieldDropsWholeEvent() throws Exception {
    runPipelineTest(
        "/test_dag_single_event_two_python_fields.yml",
        "/test_single_event_two_python_fields_input.json",
        "/test_single_event_two_python_fields_expected.json");
  }

  @Test
  public void testSkipFailedFields_oneInvalidFieldDropsOnlyThatField() throws Exception {
    runPipelineTest(
        "/test_dag_skip_failed_fields.yml",
        "/test_skip_failed_fields_input.json",
        "/test_skip_failed_fields_expected.json");
  }

  @Test
  public void testMain() throws Exception {
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
      String output = testOut.toString();
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
  }

  private void runPipelineTest(String dagResource, String inputResource, String expectedResource)
      throws Exception {
    String dagDefStr = MiscUtils.loadStringFromResource(dagResource);
    String dagDefBase64Str = MiscUtils.toBase64String(dagDefStr.getBytes());
    String[] args = {"-d", dagDefBase64Str, "-id", "test_job", "-s", "my_service", "-e", "my_env"};

    String inputJson = MiscUtils.loadStringFromResource(inputResource);
    String expectedJson = MiscUtils.loadStringFromResource(expectedResource);
    List<Map<String, Object>> expectedObjects =
        Objects.requireNonNull(fromJsonString(expectedJson, new TypeReference<>() {}));

    try (InputStream in = new ByteArrayInputStream((inputJson + "\n").getBytes());
        ByteArrayOutputStream testOut = new ByteArrayOutputStream();
        PrintStream psOut = new PrintStream(testOut)) {

      System.setIn(in);
      System.setOut(psOut);

      assertDoesNotThrow(
          () -> Main.main(args), "Pipeline should not throw on partial data failures");

      List<Map<String, Object>> actualObjects =
          testOut
              .toString()
              .lines()
              .filter(l -> l.startsWith("{\""))
              .map(l -> fromJsonString(l, new TypeReference<Map<String, Object>>() {}))
              .toList();

      assertEquals(expectedObjects.size(), actualObjects.size());
      assertEquals(new HashSet<>(expectedObjects), new HashSet<>(actualObjects));
    }
  }
}
