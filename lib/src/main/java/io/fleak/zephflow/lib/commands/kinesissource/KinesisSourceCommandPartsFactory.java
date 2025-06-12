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
package io.fleak.zephflow.lib.commands.kinesissource;

import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredentialOpt;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.compression.DecompressorFactory;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

@Slf4j
public class KinesisSourceCommandPartsFactory extends SourceCommandPartsFactory<SerializedEvent> {

  private final JobContext jobContext;

  public KinesisSourceCommandPartsFactory(
      MetricClientProvider metricClientProvider, JobContext jobContext) {
    super(metricClientProvider);
    this.jobContext = jobContext;
  }

  @Override
  public Fetcher<SerializedEvent> createFetcher(CommandConfig commandConfig) {
    KinesisSourceDto.Config config = (KinesisSourceDto.Config) commandConfig;
    AwsCredentialsProvider credentialsProvider = getAwsCredentialsProvider(config);
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher(config, credentialsProvider);
    fetcher.start();
    return fetcher;
  }

  private @Nullable AwsCredentialsProvider getAwsCredentialsProvider(
      KinesisSourceDto.Config config) {
    AwsCredentialsProvider credentialsProvider = null;
    var credentialId = StringUtils.trimToNull(config.getCredentialId());
    if (credentialId != null) {
      var credentialsOpt =
          lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());
      if (credentialsOpt.isPresent()) {
        var credentials = credentialsOpt.get();
        credentialsProvider =
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(credentials.getUsername(), credentials.getPassword()));
      }
    }
    return credentialsProvider;
  }

  @Override
  public RawDataConverter<SerializedEvent> createRawDataConverter(CommandConfig commandConfig) {
    if (!(commandConfig instanceof KinesisSourceDto.Config config)) {
      throw new IllegalArgumentException("Expected KinesisSourceDto.Config");
    }

    return new BytesRawDataConverter(
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer(),
        DecompressorFactory.getDecompressor(config.getCompressionTypes()));
  }

  @Override
  public RawDataEncoder<SerializedEvent> createRawDataEncoder(CommandConfig commandConfig) {
    return new BytesRawDataEncoder();
  }
}
