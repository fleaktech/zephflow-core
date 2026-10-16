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

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.FleakGauge;
import io.fleak.zephflow.api.metric.FleakStopWatch;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

public final class RunSummaryMetricClientProvider implements MetricClientProvider {

  private final MetricClientProvider delegate;
  private final ConcurrentMap<String, LongAdder> counterTotals = new ConcurrentHashMap<>();
  // node_id -> totals of that node's counters; only nodes with a sink_output_count are sinks
  private final ConcurrentMap<String, NodeTotals> nodeTotals = new ConcurrentHashMap<>();

  public RunSummaryMetricClientProvider(MetricClientProvider delegate) {
    this.delegate = delegate;
  }

  @Override
  public FleakCounter counter(String name, Map<String, String> tags) {
    FleakCounter delegateCounter = delegate.counter(name, tags);
    LongAdder totalForName = counterTotals.computeIfAbsent(name, counterName -> new LongAdder());
    String nodeId = tags == null ? null : tags.get(METRIC_TAG_NODE_ID);
    NodeTotals node =
        nodeId == null
            ? null
            : nodeTotals.computeIfAbsent(
                nodeId, id -> new NodeTotals(tags.get(METRIC_TAG_COMMAND_NAME)));
    LongAdder nodeTotalForName =
        node == null ? null : node.counters.computeIfAbsent(name, counterName -> new LongAdder());
    return new FleakCounter() {
      @Override
      public void increase(Map<String, String> additionalTags) {
        totalForName.increment();
        if (nodeTotalForName != null) nodeTotalForName.increment();
        delegateCounter.increase(additionalTags);
      }

      @Override
      public void increase(long n, Map<String, String> additionalTags) {
        totalForName.add(n);
        if (nodeTotalForName != null) nodeTotalForName.add(n);
        delegateCounter.increase(n, additionalTags);
      }

      @Override
      public void recordErrorSample(String errorMessage) {
        if (node != null && errorMessage != null && !errorMessage.isBlank()) {
          node.firstError.compareAndSet(null, errorMessage);
        }
        delegateCounter.recordErrorSample(errorMessage);
      }
    };
  }

  @Override
  public <T> FleakGauge<T> gauge(String name, Map<String, String> tags, T monitoredValue) {
    return delegate.gauge(name, tags, monitoredValue);
  }

  @Override
  public FleakStopWatch stopWatch(String name, Map<String, String> tags) {
    return delegate.stopWatch(name, tags);
  }

  @Override
  public void close() {
    delegate.close();
  }

  public RunSummary summarize() {
    Map<String, Long> totals = new HashMap<>();
    counterTotals.forEach((counterName, adder) -> totals.put(counterName, adder.sum()));
    List<RunSummary.SinkSummary> sinks =
        nodeTotals.entrySet().stream()
            .filter(e -> e.getValue().counters.containsKey(METRIC_NAME_SINK_OUTPUT_COUNT))
            .map(e -> e.getValue().toSinkSummary(e.getKey()))
            .sorted(Comparator.comparing(RunSummary.SinkSummary::nodeId))
            .toList();
    return new RunSummary(totals, sinks);
  }

  private static final class NodeTotals {
    private final String commandName;
    private final ConcurrentMap<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final AtomicReference<String> firstError = new AtomicReference<>();

    private NodeTotals(String commandName) {
      this.commandName = commandName;
    }

    private long total(String counterName) {
      LongAdder adder = counters.get(counterName);
      return adder == null ? 0 : adder.sum();
    }

    private RunSummary.SinkSummary toSinkSummary(String nodeId) {
      long errorCount = total(METRIC_NAME_SINK_ERROR_COUNT);
      return new RunSummary.SinkSummary(
          nodeId,
          commandName,
          total(METRIC_NAME_INPUT_EVENT_COUNT),
          total(METRIC_NAME_SINK_OUTPUT_COUNT),
          errorCount,
          errorCount > 0 ? firstError.get() : null);
    }
  }
}
