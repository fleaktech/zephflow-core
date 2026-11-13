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
package io.fleak.zephflow.lib.commands.s3;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.lib.aws.AwsUtils.parseRegion;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.sink.SinkEncoding;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import java.util.Set;
import software.amazon.awssdk.regions.Region;

/** Created by bolei on 9/3/24 */
public class S3SinkConfigValidator implements ConfigValidator {

  public static final WriterCapability CAPABILITY =
      new WriterCapability(
          COMMAND_NAME_S3_SINK,
          Unit.MANY_RECORDS,
          Set.of("csv", "json", "kv", "parquet", "text"),
          Set.of(JsonFormatConfig.Mode.ARRAY, JsonFormatConfig.Mode.LINES),
          false,
          Set.of(DecompressionCodec.GZIP));

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    S3SinkDto.Config config = (S3SinkDto.Config) commandConfig;
    try {
      parseRegion(config.getRegionStr());
    } catch (RuntimeException failure) {
      throw ConfigurationValidationException.at(
          of("regionStr"),
          failure,
          "Unknown AWS region; allowed values: "
              + Region.regions().stream().map(Region::id).sorted().toList()
              + ".");
    }
    if (enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }

    if (!config.isBatching()) {
      throw ConfigurationValidationException.at(
          of("batching"),
          new IllegalArgumentException(
              "s3sink no longer supports batching false; it always writes one file per batch"),
          "batching must be true; s3sink writes one file per batch.");
    }
    if (config.getBatchSize() <= 0) {
      throw ConfigurationValidationException.at(
          of("batchSize"),
          new IllegalArgumentException(
              String.format(
                  "Batch size must be positive when batching is enabled, but found: %d. "
                      + "If omitted, ensure default is applied correctly.",
                  config.getBatchSize())),
          "batchSize must be greater than 0.");
    }

    if (config.isStoreAndForwardEnabled()) {
      require(
          config.getLocalStoreMaxBytes() > 0,
          of("localStoreMaxBytes"),
          "localStoreMaxBytes must be greater than 0 when storeAndForwardEnabled is true.",
          "localStoreMaxBytes must be positive when storeAndForwardEnabled. Got: %s",
          config.getLocalStoreMaxBytes());
      require(
          config.getForwardRetryIntervalMillis() > 0,
          of("forwardRetryIntervalMillis"),
          "forwardRetryIntervalMillis must be greater than 0 when storeAndForwardEnabled is true.",
          "forwardRetryIntervalMillis must be positive when storeAndForwardEnabled. Got: %s",
          config.getForwardRetryIntervalMillis());
    }

    SinkEncoding.validate(config.getEncoding(), CAPABILITY);
  }
}
