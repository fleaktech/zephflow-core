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
package io.fleak.zephflow.api.metric;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class MetricConfigToStringTest {

  @Test
  void influxDbV2ConfigLeavesOutTheToken() {
    InfluxDBV2MetricSender.InfluxDBV2Config config = new InfluxDBV2MetricSender.InfluxDBV2Config();
    config.setUrl("http://influx");
    config.setOrg("org");
    config.setBucket("bucket");
    config.setMeasurement("measurement");
    config.setToken("secret-value");

    assertEquals(
        "InfluxDBV2MetricSender.InfluxDBV2Config(url=http://influx, org=org, bucket=bucket,"
            + " measurement=measurement, batchSize=5000, flushInterval=500, bufferLimit=50000,"
            + " retryInterval=5000, maxRetries=3)",
        config.toString());
  }

  @Test
  void influxDbConfigLeavesOutThePassword() {
    InfluxDBMetricSender.InfluxDBConfig config = new InfluxDBMetricSender.InfluxDBConfig();
    config.setUrl("http://influx");
    config.setDatabase("database");
    config.setMeasurement("measurement");
    config.setUsername("user");
    config.setPassword("secret-value");
    config.setRetentionPolicy("policy");

    assertEquals(
        "InfluxDBMetricSender.InfluxDBConfig(url=http://influx, database=database,"
            + " measurement=measurement, username=user, retentionPolicy=policy)",
        config.toString());
  }

  @Test
  void splunkConfigLeavesOutTheToken() {
    SplunkMetricSender.SplunkConfig config =
        SplunkMetricSender.SplunkConfig.builder()
            .hecUrl("https://splunk")
            .token("secret-value")
            .source("source")
            .index("index")
            .build();

    assertEquals(
        "SplunkMetricSender.SplunkConfig(hecUrl=https://splunk, source=source, index=index,"
            + " batchSize=100, queueCapacity=10000, flushIntervalSeconds=5, maxRetries=3,"
            + " initialRetryDelayMs=100, maxRetryDelayMs=30000, httpConnectTimeoutSeconds=10,"
            + " httpRequestTimeoutSeconds=30, shutdownTimeoutSeconds=10, flushTimeoutSeconds=5,"
            + " maxConcurrentRequests=5)",
        config.toString());
  }
}
