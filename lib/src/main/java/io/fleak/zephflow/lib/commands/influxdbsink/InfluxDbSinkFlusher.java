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
package io.fleak.zephflow.lib.commands.influxdbsink;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Writes a batch of {@link Point}s to InfluxDB using the synchronous v2 {@link WriteApiBlocking}.
 *
 * <p>The write is all-or-nothing per call: a failure throws {@link
 * com.influxdb.exceptions.InfluxException}, which propagates so {@code SimpleSinkCommand} treats
 * the whole batch as failed (and store-and-forward, if enrolled, can buffer it). Per-record mapping
 * errors are handled earlier, in {@link InfluxDbSinkMessageProcessor}.
 */
@Slf4j
public class InfluxDbSinkFlusher implements SimpleSinkCommand.Flusher<Point> {

  private final InfluxDBClient client;
  private final WriteApiBlocking writeApi;

  public InfluxDbSinkFlusher(@NonNull InfluxDBClient client) {
    this.client = client;
    this.writeApi = client.getWriteApiBlocking();
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<Point> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    List<Point> points = preparedInputEvents.preparedList();
    if (points.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    long flushedDataSize = 0;
    for (Point point : points) {
      flushedDataSize += point.toLineProtocol().getBytes(StandardCharsets.UTF_8).length;
    }

    writeApi.writePoints(points);

    return new SimpleSinkCommand.FlushResult(points.size(), flushedDataSize, List.of());
  }

  @Override
  public void close() {
    client.close();
    log.info("InfluxDbSinkFlusher closed successfully");
  }
}
