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
package io.fleak.zephflow.api;

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/** Created by bolei on 2/14/24 */
public abstract class ScalarSinkCommand extends OperatorCommand {
  protected ScalarSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      CommandInitializerFactory commandInitializerFactory) {
    super(nodeId, jobContext, configParser, configValidator, commandInitializerFactory);
  }

  public SinkResult writeToSink(
      List<RecordFleakData> events,
      @NonNull String callingUser,
      MetricClientProvider metricClientProvider) {
    lazyInitialize(metricClientProvider);
    return doWriteToSink(events, callingUser);
  }

  protected abstract SinkResult doWriteToSink(
      List<RecordFleakData> events, @NonNull String callingUser);

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  public static class SinkResult {
    int inputCount;
    int successCount;
    List<ErrorOutput> failureEvents = new ArrayList<>();

    public void merge(SinkResult that) {
      this.inputCount += that.inputCount;
      this.successCount += that.successCount;
      this.failureEvents.addAll(that.failureEvents);
    }

    public long errorCount() {
      return inputCount - successCount;
    }
  }
}
