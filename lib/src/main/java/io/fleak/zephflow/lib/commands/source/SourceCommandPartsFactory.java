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
package io.fleak.zephflow.lib.commands.source;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.CommandPartsFactory;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.S3DlqWriter;

/** Created by bolei on 9/24/24 */
public abstract class SourceCommandPartsFactory<T> extends CommandPartsFactory {

  protected SourceCommandPartsFactory(MetricClientProvider metricClientProvider) {
    super(metricClientProvider);
  }

  public abstract Fetcher<T> createFetcher(CommandConfig commandConfig);

  public abstract RawDataConverter<T> createRawDataConverter(CommandConfig commandConfig);

  public abstract RawDataEncoder<T> createRawDataEncoder(CommandConfig commandConfig);

  public DlqWriter createDlqWriter(JobContext.DlqConfig dlqConfig) {

    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      return S3DlqWriter.createS3DlqWriter(s3DlqConfig);
    }

    // TODO
    throw new UnsupportedOperationException("unsupported dlq type: " + dlqConfig);
  }
}
