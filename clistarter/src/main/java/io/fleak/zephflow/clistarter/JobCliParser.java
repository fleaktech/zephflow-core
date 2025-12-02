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
import io.fleak.zephflow.api.metric.InfluxDBMetricSender;
import io.fleak.zephflow.api.metric.SplunkMetricSender;
import io.fleak.zephflow.lib.utils.MiscUtils;
import io.fleak.zephflow.runner.JobConfig;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.Getter;
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

  // InfluxDB related options
  private static final Option METRIC_CLIENT_TYPE_OPT =
      Option.builder()
          .longOpt("metric-client-type")
          .desc("Metric client type (" + MetricClientType.getAllValues() + ")")
          .hasArg()
          .build();

  private static final Option INFLUXDB_URL_OPT =
      Option.builder().longOpt("influxdb-url").desc("InfluxDB URL").hasArg().build();

  private static final Option INFLUXDB_DATABASE_OPT =
      Option.builder().longOpt("influxdb-database").desc("InfluxDB database name").hasArg().build();

  private static final Option INFLUXDB_MEASUREMENT_OPT =
      Option.builder()
          .longOpt("influxdb-measurement")
          .desc("InfluxDB measurement")
          .hasArg()
          .build();

  private static final Option INFLUXDB_USERNAME_OPT =
      Option.builder().longOpt("influxdb-username").desc("InfluxDB username").hasArg().build();

  private static final Option INFLUXDB_PASSWORD_OPT =
      Option.builder().longOpt("influxdb-password").desc("InfluxDB password").hasArg().build();

  private static final Option INFLUXDB_RETENTION_POLICY_OPT =
      Option.builder()
          .longOpt("influxdb-retention-policy")
          .desc("InfluxDB retention policy")
          .hasArg()
          .build();

  // Splunk related options
  private static final Option SPLUNK_HEC_URL_OPT =
      Option.builder().longOpt("splunk-hec-url").desc("Splunk HEC URL").hasArg().build();

  private static final Option SPLUNK_TOKEN_OPT =
      Option.builder().longOpt("splunk-token").desc("Splunk HEC token").hasArg().build();

  private static final Option SPLUNK_SOURCE_OPT =
      Option.builder().longOpt("splunk-source").desc("Splunk source").hasArg().build();

  private static final Option SPLUNK_INDEX_OPT =
      Option.builder().longOpt("splunk-index").desc("Splunk index name").hasArg().build();

  static {
    CLI_OPTIONS = new Options();
    CLI_OPTIONS
        .addOption(ID_OPT)
        .addOption(DAG_OPT)
        .addOption(DAG_FILE_OPT)
        .addOption(SERVICE_OPT)
        .addOption(ENV_OPT)
        .addOption(METRIC_CLIENT_TYPE_OPT)
        .addOption(INFLUXDB_URL_OPT)
        .addOption(INFLUXDB_DATABASE_OPT)
        .addOption(INFLUXDB_MEASUREMENT_OPT)
        .addOption(INFLUXDB_USERNAME_OPT)
        .addOption(INFLUXDB_PASSWORD_OPT)
        .addOption(INFLUXDB_RETENTION_POLICY_OPT)
        .addOption(SPLUNK_HEC_URL_OPT)
        .addOption(SPLUNK_TOKEN_OPT)
        .addOption(SPLUNK_SOURCE_OPT)
        .addOption(SPLUNK_INDEX_OPT);
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

  public static MetricClientType getMetricClientType(String[] args) {
    CommandLineParser commandLineParser = new DefaultParser();
    try {
      CommandLine commandLine = commandLineParser.parse(CLI_OPTIONS, args);
      String value = commandLine.getOptionValue("metric-client-type");
      return MetricClientType.fromValue(value);
    } catch (ParseException e) {
      for (int i = 0; i < args.length - 1; i++) {
        if ("--metric-client-type".equals(args[i])) {
          return MetricClientType.fromValue(args[i + 1]);
        }
      }
      return MetricClientType.NOOP;
    }
  }

  public static InfluxDBMetricSender.InfluxDBConfig parseInfluxDBConfig(String[] args)
      throws ParseException {

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(CLI_OPTIONS, args);

    InfluxDBMetricSender.InfluxDBConfig config = new InfluxDBMetricSender.InfluxDBConfig();

    if (commandLine.hasOption("influxdb-url")) {
      config.setUrl(commandLine.getOptionValue("influxdb-url"));
    }
    if (commandLine.hasOption("influxdb-database")) {
      config.setDatabase(commandLine.getOptionValue("influxdb-database"));
    }
    if (commandLine.hasOption("influxdb-measurement")) {
      config.setMeasurement(commandLine.getOptionValue("influxdb-measurement"));
    }
    if (commandLine.hasOption("influxdb-username")) {
      config.setUsername(commandLine.getOptionValue("influxdb-username"));
    }
    if (commandLine.hasOption("influxdb-password")) {
      config.setPassword(commandLine.getOptionValue("influxdb-password"));
    }
    if (commandLine.hasOption("influxdb-retention-policy")) {
      config.setRetentionPolicy(commandLine.getOptionValue("influxdb-retention-policy"));
    }

    return config;
  }

  public static SplunkMetricSender.SplunkConfig parseSplunkConfig(String[] args)
      throws ParseException {

    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine = commandLineParser.parse(CLI_OPTIONS, args);

    SplunkMetricSender.SplunkConfig.SplunkConfigBuilder config =
        SplunkMetricSender.SplunkConfig.builder();

    if (commandLine.hasOption("splunk-hec-url")) {
      config.hecUrl(commandLine.getOptionValue("splunk-hec-url"));
    }
    if (commandLine.hasOption("splunk-token")) {
      config.token(commandLine.getOptionValue("splunk-token"));
    }
    if (commandLine.hasOption("splunk-source")) {
      config.source(commandLine.getOptionValue("splunk-source"));
    }
    if (commandLine.hasOption("splunk-index")) {
      config.index(commandLine.getOptionValue("splunk-index"));
    }

    return config.build();
  }

  @Getter
  public enum MetricClientType {
    INFLUXDB("influxdb"),
    SPLUNK("splunk"),
    NOOP("noop");

    private final String value;

    MetricClientType(String value) {
      this.value = value;
    }

    /**
     * Parse string value to MetricClientType enum
     *
     * @param value the string value
     * @return MetricClientType enum, defaults to NOOP if not found
     */
    public static MetricClientType fromValue(String value) {
      if (value == null || value.trim().isEmpty()) {
        return NOOP;
      }

      for (MetricClientType type : MetricClientType.values()) {
        if (type.value.equalsIgnoreCase(value.trim())) {
          return type;
        }
      }
      return NOOP;
    }

    /**
     * Get all supported values as comma-separated string for help text
     *
     * @return comma-separated string of all values
     */
    public static String getAllValues() {
      return Arrays.stream(MetricClientType.values())
          .map(MetricClientType::getValue)
          .collect(Collectors.joining(", "));
    }

    @Override
    public String toString() {
      return value;
    }
  }
}
