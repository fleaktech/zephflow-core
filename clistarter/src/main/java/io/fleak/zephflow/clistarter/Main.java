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

import io.fleak.zephflow.api.metric.InfluxDBMetricClientProvider;
import io.fleak.zephflow.api.metric.InfluxDBMetricSender;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.metric.SplunkMetricClientProvider;
import io.fleak.zephflow.api.metric.SplunkMetricSender;
import io.fleak.zephflow.runner.DagExecutor;
import io.fleak.zephflow.runner.JobConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.ParseException;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;

/** Created by bolei on 9/26/24 */
@Slf4j
public class Main {

  public static void main(String[] args) throws Exception {
    try {
      JobConfig jobConfig = JobCliParser.parseArgs(args);
      try (MetricClientProvider metricClientProvider = createMetricClientProvider(args)) {
        DagExecutor dagExecutor = DagExecutor.createDagExecutor(jobConfig, metricClientProvider);
        dagExecutor.executeDag();
      }
    } catch (ParseException cliParseException) {
      JobCliParser.printUsage("pipelinejob");
      System.exit(1);
    }
  }

  private static MetricClientProvider createMetricClientProvider(String[] args) {
    try {
      JobCliParser.MetricClientType metricClientType = JobCliParser.getMetricClientType(args);
      log.info("Selected metric client:  {}", metricClientType.getValue());

      return switch (metricClientType) {
        case INFLUXDB -> createInfluxDBMetricClientProvider(args);
        case SPLUNK -> createSplunkMetricClientProvider(args);
        case NOOP -> new MetricClientProvider.NoopMetricClientProvider();
      };
    } catch (Exception e) {
      log.error("Failed to create metric client provider: {}", e.getMessage());
      return new MetricClientProvider.NoopMetricClientProvider();
    }
  }

  private static MetricClientProvider createInfluxDBMetricClientProvider(String[] args)
      throws ParseException {
    try {
      InfluxDBMetricSender.InfluxDBConfig influxDBConfig = JobCliParser.parseInfluxDBConfig(args);
      log.info("influxDB config:{}", influxDBConfig);

      // Create InfluxDB client with optional authentication
      InfluxDB influxDB = createInfluxDBClient(influxDBConfig);

      // Set database and create if it doesn't exist
      setupDatabase(influxDB, influxDBConfig);

      InfluxDBMetricSender influxDBMetricSender =
          new InfluxDBMetricSender(influxDBConfig, influxDB);
      return new InfluxDBMetricClientProvider(influxDBMetricSender);
    } catch (Exception e) {
      log.error("Failed to initialize InfluxDB: {}", e.getMessage());
      throw e;
    }
  }

  private static InfluxDB createInfluxDBClient(InfluxDBMetricSender.InfluxDBConfig config) {
    log.debug("Creating InfluxDB client for URL: {}", config.getUrl());

    InfluxDB client;
    if (hasCredentials(config)) {
      log.debug("Using username/password authentication");
      client = InfluxDBFactory.connect(config.getUrl(), config.getUsername(), config.getPassword());
    } else {
      log.debug("Using connection without authentication");
      client = InfluxDBFactory.connect(config.getUrl());
    }

    // Configure client for better performance
    client.setLogLevel(InfluxDB.LogLevel.NONE);
    client.enableBatch(100, 200, java.util.concurrent.TimeUnit.MILLISECONDS);

    return client;
  }

  private static boolean hasCredentials(InfluxDBMetricSender.InfluxDBConfig config) {
    return config.getUsername() != null
        && !config.getUsername().trim().isEmpty()
        && config.getPassword() != null
        && !config.getPassword().trim().isEmpty();
  }

  private static void setupDatabase(InfluxDB influxDB, InfluxDBMetricSender.InfluxDBConfig config) {
    if (config.getDatabase() != null && !config.getDatabase().trim().isEmpty()) {
      String databaseName = config.getDatabase().trim();
      log.info("Setting up InfluxDB database: {}", databaseName);

      try {
        log.debug("Creating database '{}' if it doesn't exist", databaseName);
        influxDB.query(new Query("CREATE DATABASE \"" + databaseName + "\""));
        log.info("Successfully created/verified database: {}", databaseName);

        log.debug("Setting active database to: {}", databaseName);
        influxDB.setDatabase(databaseName);
        log.info("Successfully set active database to: {}", databaseName);

      } catch (Exception e) {
        log.error("Failed to setup InfluxDB database '{}': {}", databaseName, e.getMessage(), e);
        throw e;
      }
    } else {
      log.warn("InfluxDB database name is null or empty, skipping database setup");
    }
  }

  private static MetricClientProvider createSplunkMetricClientProvider(String[] args)
      throws ParseException {
    try {
      SplunkMetricSender.SplunkConfig splunkConfig = JobCliParser.parseSplunkConfig(args);
      log.info("Splunk config: {}", splunkConfig);

      SplunkMetricSender splunkMetricSender = new SplunkMetricSender(splunkConfig);
      return new SplunkMetricClientProvider(splunkMetricSender);
    } catch (Exception e) {
      log.error("Failed to initialize Splunk: {}", e.getMessage());
      throw e;
    }
  }
}
