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
import io.fleak.zephflow.api.metric.InfluxDBMetricClientProvider;
import io.fleak.zephflow.api.metric.InfluxDBMetricSender;
import io.fleak.zephflow.api.metric.MetricClientProvider;
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

      MetricClientProvider metricClientProvider;
      if (JobCliParser.hasInfluxDBFlag(args)) {
        try {
          InfluxDBMetricSender.InfluxDBConfig influxDBConfig =
              JobCliParser.parseInfluxDBConfig(args);
          log.info("Start Initialize InfluxDB metric client, InfluxDB config: {}", influxDBConfig);

          InfluxDBClient influxDBClient =
              InfluxDBClientFactory.create(
                  influxDBConfig.getUrl(),
                  influxDBConfig.getToken().toCharArray(),
                  influxDBConfig.getOrg(),
                  influxDBConfig.getBucket());
          InfluxDBMetricSender influxDBMetricSender =
              new InfluxDBMetricSender(influxDBConfig, influxDBClient);
          metricClientProvider = new InfluxDBMetricClientProvider(influxDBMetricSender);
          log.info("Initialized InfluxDB metric client, InfluxDB config: {}", influxDBConfig);
        } catch (Exception e) {
          log.error("Failed to initialize InfluxDB metric client: {}", e.getMessage());
          metricClientProvider = new MetricClientProvider.NoopMetricClientProvider();
        }
      } else {
        metricClientProvider = new MetricClientProvider.NoopMetricClientProvider();
      }

      DagExecutor dagExecutor = DagExecutor.createDagExecutor(jobConfig, metricClientProvider);
      dagExecutor.executeDag();
    } catch (ParseException cliParseException) {
      JobCliParser.printUsage("pipelinejob");
      System.exit(1);
    }
  }
}
