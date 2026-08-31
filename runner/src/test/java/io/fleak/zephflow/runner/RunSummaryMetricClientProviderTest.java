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
package io.fleak.zephflow.runner;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static io.fleak.zephflow.runner.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.FleakGauge;
import io.fleak.zephflow.api.metric.FleakStopWatch;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RunSummaryMetricClientProviderTest {

  @Test
  void accumulatesCounterTotalsByNameAcrossTagsAndInstances() {
    MetricClientProvider delegate = mock(MetricClientProvider.class);
    FleakCounter delegateCounter = mock(FleakCounter.class);
    when(delegate.counter(any(), any())).thenReturn(delegateCounter);

    RunSummaryMetricClientProvider provider = new RunSummaryMetricClientProvider(delegate);

    provider.counter("input_event_count", Map.of("node_id", "a")).increase(5, Map.of());
    provider.counter("input_event_count", Map.of("node_id", "b")).increase(Map.of());
    provider.counter("sink_output_count", Map.of()).increase(3, Map.of());

    RunSummary summary = provider.summarize();
    assertEquals(6L, summary.counterTotal("input_event_count"));
    assertEquals(3L, summary.counterTotal("sink_output_count"));
    assertEquals(0L, summary.counterTotal("nonexistent"));

    verify(delegateCounter).increase(eq(5L), any());
    verify(delegateCounter).increase(any());
    verify(delegateCounter).increase(eq(3L), any());
  }

  @Test
  void delegatesGaugeStopWatchAndClose() {
    MetricClientProvider delegate = mock(MetricClientProvider.class);
    @SuppressWarnings("unchecked")
    FleakGauge<Long> gauge = mock(FleakGauge.class);
    FleakStopWatch stopWatch = mock(FleakStopWatch.class);
    when(delegate.gauge(eq("g"), any(), anyLong())).thenReturn(gauge);
    when(delegate.stopWatch(eq("sw"), any())).thenReturn(stopWatch);

    RunSummaryMetricClientProvider provider = new RunSummaryMetricClientProvider(delegate);

    assertSame(gauge, provider.gauge("g", Map.of(), 1L));
    assertSame(stopWatch, provider.stopWatch("sw", Map.of()));
    provider.close();
    verify(delegate).close();
  }

  @Test
  void summaryTextReportsCoreCounters() {
    RunSummary summary =
        new RunSummary(
            Map.of(
                METRIC_NAME_PIPELINE_INPUT_EVENT, 0L,
                METRIC_NAME_INPUT_DESER_ERR_COUNT, 2L,
                METRIC_NAME_SINK_OUTPUT_COUNT, 0L));

    assertEquals(
        "input events: 0, deserialize failures: 2, output events: 0", summary.summaryText());
  }

  @Test
  void summaryTextIncludesOptionalCountersOnlyWhenPositive() {
    RunSummary summary =
        new RunSummary(
            Map.of(
                METRIC_NAME_PIPELINE_INPUT_EVENT, 10L,
                METRIC_NAME_SINK_OUTPUT_COUNT, 7L,
                METRIC_NAME_SKIPPED_OBJECT_COUNT, 1L,
                METRIC_NAME_SINK_ERROR_COUNT, 3L,
                METRIC_NAME_PIPELINE_ERROR_EVENT, 4L));

    assertEquals(
        "input events: 10, deserialize failures: 0, output events: 7,"
            + " skipped objects: 1, sink errors: 3, processing errors: 4",
        summary.summaryText());
  }
}
