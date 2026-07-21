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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import com.influxdb.exceptions.InfluxException;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InfluxDbSinkFlusherTest {

  private InfluxDBClient client;
  private WriteApiBlocking writeApi;
  private InfluxDbSinkFlusher flusher;

  @BeforeEach
  void setUp() {
    client = mock(InfluxDBClient.class);
    writeApi = mock(WriteApiBlocking.class);
    when(client.getWriteApiBlocking()).thenReturn(writeApi);
    flusher = new InfluxDbSinkFlusher(client);
  }

  private static SimpleSinkCommand.PreparedInputEvents<Point> prepared(Point... points) {
    var prepared = new SimpleSinkCommand.PreparedInputEvents<Point>();
    RecordFleakData raw = (RecordFleakData) FleakData.wrap(Map.of("k", "v"));
    for (Point p : points) {
      prepared.add(raw, p);
    }
    return prepared;
  }

  @Test
  void writesAllPointsAndReportsSuccess() throws Exception {
    Point p1 = Point.measurement("m").addField("v", 1L);
    Point p2 = Point.measurement("m").addField("v", 2L);

    var result = flusher.flush(prepared(p1, p2), Map.of());

    verify(writeApi).writePoints(List.of(p1, p2));
    assertEquals(2, result.successCount());
    assertTrue(result.flushedDataSize() > 0);
    assertTrue(result.errorOutputList().isEmpty());
  }

  @Test
  void emptyBatchDoesNotCallClient() throws Exception {
    var result = flusher.flush(prepared(), Map.of());

    assertEquals(0, result.successCount());
    verify(writeApi, never()).writePoints(anyList());
  }

  @Test
  void propagatesWriteExceptionAsWholeBatchFailure() {
    doThrow(new InfluxException("boom")).when(writeApi).writePoints(anyList());
    Point p1 = Point.measurement("m").addField("v", 1L);

    assertThrows(Exception.class, () -> flusher.flush(prepared(p1), Map.of()));
  }

  @Test
  void closeClosesClient() {
    flusher.close();
    verify(client).close();
  }
}
