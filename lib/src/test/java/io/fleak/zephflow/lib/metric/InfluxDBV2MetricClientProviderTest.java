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
package io.fleak.zephflow.lib.metric;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.fleak.zephflow.api.metric.*;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class InfluxDBV2MetricClientProviderTest {

  @Mock private InfluxDBV2MetricSender mockInfluxDBV2MetricSender;

  private InfluxDBV2MetricClientProvider influxDBV2MetricClientProvider;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    influxDBV2MetricClientProvider = new InfluxDBV2MetricClientProvider(mockInfluxDBV2MetricSender);
  }

  @Test
  void counterReturnsInfluxDBV2FleakCounter() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    FleakCounter counter = influxDBV2MetricClientProvider.counter("test_counter", tags);

    assertNotNull(counter);
    assertInstanceOf(InfluxDBV2FleakCounter.class, counter);
  }

  @Test
  void stopWatchReturnsInfluxDBV2StopWatch() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    FleakStopWatch stopWatch = influxDBV2MetricClientProvider.stopWatch("test_stopwatch", tags);

    assertNotNull(stopWatch);
    assertInstanceOf(InfluxDBV2StopWatch.class, stopWatch);
  }

  @Test
  void counterWithNullTagsReturnsInfluxDBV2FleakCounter() {
    FleakCounter counter = influxDBV2MetricClientProvider.counter("test_counter", null);

    assertNotNull(counter);
    assertInstanceOf(InfluxDBV2FleakCounter.class, counter);
  }

  @Test
  void counterWithEmptyTagsReturnsInfluxDBV2FleakCounter() {
    Map<String, String> emptyTags = new HashMap<>();

    FleakCounter counter = influxDBV2MetricClientProvider.counter("test_counter", emptyTags);

    assertNotNull(counter);
    assertInstanceOf(InfluxDBV2FleakCounter.class, counter);
  }
}
