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

import static io.fleak.zephflow.runner.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.FleakStopWatch;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import java.util.Map;

/** Created by bolei on 3/5/25 */
public record DagRunCounters(
    FleakCounter inputEventCounter,
    FleakCounter inputDataSizeCounter,
    FleakCounter totalDataSizeCounter,
    FleakCounter outputEventCounter,
    FleakCounter errorEventCounter,
    FleakCounter numRequests,
    FleakStopWatch fleakStopWatch) {

  public void increaseInputEventCounter(long n, Map<String, String> callingUserTag) {
    inputEventCounter.increase(n, callingUserTag);
  }

  public void increaseInputByteSizeCounter(long n, Map<String, String> callingUserTag) {
    inputDataSizeCounter.increase(n, callingUserTag);
  }

  public void increaseTotalDataSizeCounter(long n, Map<String, String> callingUserTag) {
    totalDataSizeCounter.increase(n, callingUserTag);
  }

  public void increaseOutputEventCounter(long n, Map<String, String> callingUserTag) {
    outputEventCounter.increase(n, callingUserTag);
  }

  public void increaseErrorEventCounter(long n, Map<String, String> callingUserTag) {
    errorEventCounter.increase(n, callingUserTag);
  }

  public void increaseNumRequestCounter(long n, Map<String, String> callingUserTag) {
    numRequests.increase(n, callingUserTag);
  }

  public void startStopWatch() {
    fleakStopWatch.start();
  }

  public void stopStopWatch(Map<String, String> callingUserTag) {
    fleakStopWatch.stop(callingUserTag);
  }

  @VisibleForTesting
  public static DagRunCounters createPipelineCounters(
      MetricClientProvider metricClientProvider, Map<String, String> metricTags) {
    FleakCounter inputCounter =
        metricClientProvider.counter(METRIC_NAME_PIPELINE_INPUT_EVENT, metricTags);
    FleakCounter inputSizeCounter =
        metricClientProvider.counter(METRIC_NAME_PIPELINE_INPUT_SIZE_BYTES, metricTags);
    FleakCounter totalDataSizeCounter =
        metricClientProvider.counter(METRIC_NAME_PIPELINE_TOTAL_SIZE_BYTES, metricTags);
    FleakCounter outputCounter =
        metricClientProvider.counter(METRIC_NAME_PIPELINE_OUTPUT_EVENT, metricTags);
    FleakCounter errorCounter =
        metricClientProvider.counter(METRIC_NAME_PIPELINE_ERROR_EVENT, metricTags);
    FleakCounter requestCounter =
        metricClientProvider.counter(METRIC_NAME_NUM_REQUESTS, metricTags);
    FleakStopWatch stopWatch =
        metricClientProvider.stopWatch(METRIC_NAME_REQUEST_PROCESS_TIME_MILLIS, metricTags);
    return new DagRunCounters(
        inputCounter,
        inputSizeCounter,
        totalDataSizeCounter,
        outputCounter,
        errorCounter,
        requestCounter,
        stopWatch);
  }
}
