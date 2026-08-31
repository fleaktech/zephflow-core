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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record RunSummary(Map<String, Long> counters) {

  public long counterTotal(String counterName) {
    Long total = counters.get(counterName);
    return total == null ? 0 : total;
  }

  public String summaryText() {
    List<String> parts = new ArrayList<>();
    parts.add("input events: " + counterTotal(METRIC_NAME_PIPELINE_INPUT_EVENT));
    parts.add("deserialize failures: " + counterTotal(METRIC_NAME_INPUT_DESER_ERR_COUNT));
    parts.add("output events: " + counterTotal(METRIC_NAME_SINK_OUTPUT_COUNT));
    addIfPositive(parts, "skipped objects", METRIC_NAME_SKIPPED_OBJECT_COUNT);
    addIfPositive(parts, "sink errors", METRIC_NAME_SINK_ERROR_COUNT);
    addIfPositive(parts, "processing errors", METRIC_NAME_PIPELINE_ERROR_EVENT);
    return String.join(", ", parts);
  }

  private void addIfPositive(List<String> parts, String label, String counterName) {
    long total = counterTotal(counterName);
    if (total > 0) {
      parts.add(label + ": " + total);
    }
  }
}
