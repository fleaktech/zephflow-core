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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class InfluxDBV2FleakCounterTest {

  @Test
  void increase_negativeIncrementDoesNotCorruptSubsequentCounts() {
    InfluxDBV2MetricSender sender = mock(InfluxDBV2MetricSender.class);
    InfluxDBV2FleakCounter counter =
        new InfluxDBV2FleakCounter("test_counter", Map.of(), sender, 1000, 600_000);

    counter.increase(-8, Map.of());
    counter.increase(5, Map.of());
    counter.flush();

    verify(sender).sendMetric(eq("counter"), eq("test_counter"), eq(5L), any(), any());
  }

  @Test
  void increase_negativeIncrementIsNeverSent() {
    InfluxDBV2MetricSender sender = mock(InfluxDBV2MetricSender.class);
    InfluxDBV2FleakCounter counter =
        new InfluxDBV2FleakCounter("test_counter", Map.of(), sender, 1000, 600_000);

    counter.increase(-9000, Map.of());
    counter.flush();

    verify(sender, never()).sendMetric(any(), any(), anyLong(), any(), any());
  }
}
