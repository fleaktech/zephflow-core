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

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import io.fleak.zephflow.api.metric.InfluxDBMetricClientProvider;
import io.fleak.zephflow.api.metric.InfluxDBMetricSender;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.metric.SplunkMetricClientProvider;
import io.fleak.zephflow.api.metric.SplunkMetricSender;
import io.fleak.zephflow.runner.DagExecutor;
import io.fleak.zephflow.runner.JobConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.ParseException;

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
      log.info("InfluxDB 2.x config: {}", influxDBConfig);

      InfluxDBClient influxDBClient = createInfluxDBClient(influxDBConfig);

      InfluxDBMetricSender influxDBMetricSender =
          new InfluxDBMetricSender(influxDBConfig, influxDBClient);
      return new InfluxDBMetricClientProvider(influxDBMetricSender);
    } catch (Exception e) {
      log.error("Failed to initialize InfluxDB 2.x: {}", e.getMessage());
      throw e;
    }
  }

  private static InfluxDBClient createInfluxDBClient(InfluxDBMetricSender.InfluxDBConfig config) {
    log.debug("Creating InfluxDB 2.x client for URL: {}", config.getUrl());

    if (config.getToken() == null || config.getToken().trim().isEmpty()) {
      throw new IllegalArgumentException("InfluxDB 2.x requires a token for authentication");
    }

    InfluxDBClientOptions options =
        InfluxDBClientOptions.builder()
            .url(config.getUrl())
            .authenticateToken(config.getToken().toCharArray())
            .org(config.getOrg())
            .bucket(config.getBucket())
            .build();

    InfluxDBClient client = InfluxDBClientFactory.create(options);

    log.info("InfluxDB 2.x client created successfully");
    return client;
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
