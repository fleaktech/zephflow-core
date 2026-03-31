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

import io.fleak.zephflow.api.JobContext;

public class DlqWriterFactory {

  public static DlqWriter createDlqWriter(JobContext.DlqConfig dlqConfig, String keyPrefix) {
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      return S3DlqWriter.createS3DlqWriter(s3DlqConfig, keyPrefix);
    }
    if (dlqConfig instanceof JobContext.GcsDlqConfig gcsDlqConfig) {
      return GcsDlqWriter.createGcsDlqWriter(gcsDlqConfig, keyPrefix);
    }
    throw new UnsupportedOperationException("unsupported dlq type: " + dlqConfig);
  }

  public static DlqWriter createSampleWriter(JobContext.DlqConfig dlqConfig, String keyPrefix) {
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      return S3DlqWriter.createS3SampleWriter(s3DlqConfig, keyPrefix);
    }
    if (dlqConfig instanceof JobContext.GcsDlqConfig gcsDlqConfig) {
      return GcsDlqWriter.createGcsSampleWriter(gcsDlqConfig, keyPrefix);
    }
    throw new UnsupportedOperationException("unsupported dlq type: " + dlqConfig);
  }
}
