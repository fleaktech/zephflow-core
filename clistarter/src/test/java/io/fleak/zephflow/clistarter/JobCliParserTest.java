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

import static io.fleak.zephflow.lib.utils.YamlUtils.fromYamlString;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.utils.MiscUtils;
import io.fleak.zephflow.runner.JobConfig;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Created by bolei on 3/21/25 */
class JobCliParserTest {

  @Test
  void parseArgs_loadDagFromCli() throws ParseException {
    String dagDefStr = MiscUtils.loadStringFromResource("/test_dag_stdio.yml");
    String dagDefBase64Str = MiscUtils.toBase64String(dagDefStr.getBytes());
    String[] args =
        String.format("-id test_job -s test_service -e test_env -d %s", dagDefBase64Str)
            .split("\\s+");
    JobConfig jobConfig = JobCliParser.parseArgs(args);
    assertEquals("test_job", jobConfig.getJobId());
    assertEquals("test_service", jobConfig.getService());
    assertEquals("test_env", jobConfig.getEnvironment());
    AdjacencyListDagDefinition expectedDagDef = fromYamlString(dagDefStr, new TypeReference<>() {});
    assertEquals(expectedDagDef, jobConfig.getDagDefinition());
  }

  @Test
  void parseArgs_loadDagFromFile(@TempDir Path tempDir) throws IOException, ParseException {
    // Create temporary DAG file
    Path dagFile = tempDir.resolve("test_dag.yml");
    String dagDefStr = MiscUtils.loadStringFromResource("/test_dag_stdio.yml");
    Files.writeString(dagFile, dagDefStr);

    String[] args =
        String.format("-id test_job -s test_service -e test_env -f %s", dagFile).split("\\s+");
    JobConfig jobConfig = JobCliParser.parseArgs(args);

    assertEquals("test_job", jobConfig.getJobId());
    assertEquals("test_service", jobConfig.getService());
    assertEquals("test_env", jobConfig.getEnvironment());
    AdjacencyListDagDefinition expectedDagDef = fromYamlString(dagDefStr, new TypeReference<>() {});
    assertEquals(expectedDagDef, jobConfig.getDagDefinition());
  }

  @Test
  void parseArgs_withDefaultServiceAndEnv(@TempDir Path tempDir)
      throws ParseException, IOException {
    Path dagFile = tempDir.resolve("test_dag.yml");
    String dagDefStr = MiscUtils.loadStringFromResource("/test_dag_stdio.yml");
    Files.writeString(dagFile, dagDefStr);

    String[] args = String.format("-f %s", dagFile).split("\\s+");

    JobConfig jobConfig = JobCliParser.parseArgs(args);

    assertEquals("default_service", jobConfig.getService());
    assertEquals("default_env", jobConfig.getEnvironment());
    assertTrue(jobConfig.getJobId().matches("\\w{16}"));
  }

  @Test
  void parseArgs_doesNotLogCredentialsFromDag(@TempDir Path tempDir) throws Exception {
    String secret = "fle2953-secret-api-key";
    String dagStr =
        """
        jobContext:
          otherProperties:
            my-credential:
              key: "%s"
        dag:
          - id: "src"
            commandName: "stdin"
            config:
              encodingType: "JSON_ARRAY"
            outputs: ["sink"]
          - id: "sink"
            commandName: "stdout"
            config:
              encodingType: "JSON_OBJECT"
        """
            .formatted(secret);
    Path dagFile = tempDir.resolve("dag.yml");
    Files.writeString(dagFile, dagStr);

    Logger logger = (Logger) LogManager.getLogger(JobCliParser.class);
    Level originalLevel = logger.getLevel();
    List<LogEvent> events = new CopyOnWriteArrayList<>();
    AbstractAppender appender =
        new AbstractAppender("fle2953-capture", null, null, false, Property.EMPTY_ARRAY) {
          @Override
          public void append(LogEvent event) {
            events.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.ALL);
    try {
      JobCliParser.parseArgs(new String[] {"-id", "test_job", "-f", dagFile.toString()});
    } finally {
      logger.removeAppender(appender);
      logger.setLevel(originalLevel);
      appender.stop();
    }

    assertFalse(events.isEmpty(), "expected the dag summary to be logged");
    for (LogEvent event : events) {
      String message = event.getMessage().getFormattedMessage();
      assertFalse(message.contains(secret), "credential leaked into log: " + message);
      assertFalse(message.contains("otherProperties"), "jobContext leaked into log: " + message);
    }
    assertTrue(
        events.stream()
            .anyMatch(
                e -> e.getMessage().getFormattedMessage().contains("src(stdin), sink(stdout)")));
  }

  @Test
  void parseArgs_invalidBase64DagDoesNotEchoArgument() {
    String secretArg = MiscUtils.toBase64String("not: [valid dag key: fle2953-secret".getBytes());
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> JobCliParser.parseArgs(new String[] {"-id", "test_job", "-d", secretArg}));
    assertFalse(e.getMessage().contains(secretArg));
  }
}
