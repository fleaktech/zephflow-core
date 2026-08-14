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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.write.Point;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class InfluxDBV2MetricSenderTest {

  /**
   * InfluxDB identifies a point by measurement + tag set + timestamp: two writes of the same
   * counter field with identical tags in the same timestamp silently overwrite each other, losing
   * increments. Rapid successive sends must therefore never share a timestamp.
   */
  @Test
  void sendMetric_rapidCallsWithSameTagsProduceDistinctTimestamps() {
    InfluxDBClient client = mock(InfluxDBClient.class);
    WriteApi writeApi = mock(WriteApi.class);
    when(client.makeWriteApi(any(WriteOptions.class))).thenReturn(writeApi);

    InfluxDBV2MetricSender.InfluxDBV2Config config = new InfluxDBV2MetricSender.InfluxDBV2Config();
    config.setOrg("org");
    config.setBucket("bucket");
    config.setMeasurement("worker_metrics");
    InfluxDBV2MetricSender sender = new InfluxDBV2MetricSender(config, client);

    int n = 200;
    for (int i = 0; i < n; i++) {
      sender.sendMetric("counter", "input_event_count", 1000L, Map.of("node_id", "n1"), null);
    }

    ArgumentCaptor<Point> pointCaptor = ArgumentCaptor.forClass(Point.class);
    verify(writeApi, times(n)).writePoint(eq("bucket"), eq("org"), pointCaptor.capture());

    Set<String> timestamps =
        pointCaptor.getAllValues().stream()
            .map(Point::toLineProtocol)
            .map(lp -> lp.substring(lp.lastIndexOf(' ') + 1))
            .collect(Collectors.toSet());
    assertEquals(n, timestamps.size(), "points sharing a timestamp overwrite each other");
  }
}
