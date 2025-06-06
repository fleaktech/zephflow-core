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

import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static io.fleak.zephflow.lib.utils.YamlUtils.fromYamlString;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.utils.MiscUtils;
import io.fleak.zephflow.runner.JobConfig;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

/** Created by bolei on 9/26/24 */
@Slf4j
public class JobCliParser {
  private static final Options CLI_OPTIONS;
  private static final Option ID_OPT =
      Option.builder("id").longOpt("jobId").desc("Pipeline job Id string").hasArg().build();

  private static final Option DAG_OPT =
      Option.builder("d").longOpt("dag").desc("base64 encoded dag string").hasArg().build();

  private static final Option DAG_FILE_OPT =
      Option.builder("f").longOpt("dagFile").desc("path to the DAG yaml file").hasArg().build();

  private static final Option SERVICE_OPT =
      Option.builder("s").longOpt("service").desc("service").hasArg().build();

  private static final Option ENV_OPT =
      Option.builder("e").longOpt("environment").desc("environment").hasArg().build();

  static {
    CLI_OPTIONS = new Options();
    CLI_OPTIONS
        .addOption(ID_OPT)
        .addOption(DAG_OPT)
        .addOption(DAG_FILE_OPT)
        .addOption(SERVICE_OPT)
        .addOption(ENV_OPT);
  }

  public static JobConfig parseArgs(String[] args) throws ParseException {

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(CLI_OPTIONS, args);

    String id = getOptionalCommandArgValue(commandLine, "id", i -> i, generateRandomHash());
    String service = getOptionalCommandArgValue(commandLine, "s", s -> s, "default_service");
    String env = getOptionalCommandArgValue(commandLine, "e", e -> e, "default_env");

    var adjacencyListDagDefinition = getDag(commandLine);
    return JobConfig.builder()
        .jobId(id)
        .dagDefinition(adjacencyListDagDefinition)
        .service(service)
        .environment(env)
        .build();
  }

  private static AdjacencyListDagDefinition getDag(CommandLine commandLine) {

    // check if dag string (-d) is directly available
    AdjacencyListDagDefinition dagDefinition;

    dagDefinition =
        MiscUtils.getOptionalCommandArgValue(
            commandLine,
            "d",
            d -> {
              if (StringUtils.isBlank(d)) {
                return null;
              }
              try {
                String dagStr = new String(fromBase64String(d));
                return fromYamlString(dagStr, new TypeReference<>() {});
              } catch (Exception e) {
                throw new IllegalArgumentException(
                    "failed to convert -d argument into a dag: " + d);
              }
            },
            null);
    if (dagDefinition != null) {
      return dagDefinition;
    }

    // check if dag file (-f) is available
    dagDefinition =
        MiscUtils.getOptionalCommandArgValue(
            commandLine,
            "f",
            f -> {
              if (StringUtils.isBlank(f)) {
                return null;
              }
              try {
                String dagStr = Files.readString(Path.of(f));
                log.info("read content from dag file:\n {}", dagStr);
                return fromYamlString(dagStr, new TypeReference<>() {});
              } catch (Exception e) {
                throw new IllegalArgumentException("failed to load dag from file: " + f, e);
              }
            },
            null);
    if (dagDefinition != null) {
      return dagDefinition;
    }

    // try to get dag from the DAG environment variable
    String dagStr = StringUtils.trimToNull(System.getenv("DAG"));
    if (dagStr == null) {
      throw new RuntimeException("no DAG were provided");
    }
    return fromYamlString(dagStr, new TypeReference<>() {});
  }

  public static void printUsage(String prog) {
    HelpFormatter formatter = new HelpFormatter();
    String header = "Options:";
    String footer = "\n";
    formatter.printHelp(prog, header, CLI_OPTIONS, footer, true);
  }
}
