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
package io.fleak.zephflow.lib.dlq;

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_DLQ_WRITE_ERROR_COUNT;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import java.util.Map;

public class DlqWriterFactory {

  /**
   * Dead letters the writer drops because the store refused them are counted on {@code
   * dlq_write_error_count} with the given tags, which also puts them in the run summary.
   */
  public static DlqWriter createDlqWriter(
      JobContext.DlqConfig dlqConfig,
      String keyPrefix,
      MetricClientProvider metricClientProvider,
      Map<String, String> metricTags) {
    FleakCounter unwrittenDeadLetterCounter =
        metricClientProvider.counter(METRIC_NAME_DLQ_WRITE_ERROR_COUNT, metricTags);
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      return S3DlqWriter.createS3DlqWriter(s3DlqConfig, keyPrefix, unwrittenDeadLetterCounter);
    }
    if (dlqConfig instanceof JobContext.GcsDlqConfig gcsDlqConfig) {
      return GcsDlqWriter.createGcsDlqWriter(gcsDlqConfig, keyPrefix, unwrittenDeadLetterCounter);
    }
    throw unsupported(dlqConfig);
  }

  public static DlqWriter createSampleWriter(JobContext.DlqConfig dlqConfig, String keyPrefix) {
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      return S3DlqWriter.createS3SampleWriter(s3DlqConfig, keyPrefix);
    }
    if (dlqConfig instanceof JobContext.GcsDlqConfig gcsDlqConfig) {
      return GcsDlqWriter.createGcsSampleWriter(gcsDlqConfig, keyPrefix);
    }
    throw unsupported(dlqConfig);
  }

  private static UnsupportedOperationException unsupported(JobContext.DlqConfig dlqConfig) {
    return new UnsupportedOperationException(
        "unsupported dlq type: " + dlqConfig.getClass().getName());
  }
}
