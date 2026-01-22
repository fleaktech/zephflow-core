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

import static io.fleak.zephflow.lib.aws.AwsUtils.parseRegion;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.EncodingType;

/** Created by bolei on 9/3/24 */
public class S3SinkConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    S3SinkDto.Config config = (S3SinkDto.Config) commandConfig;
    parseRegion(config.getRegionStr());
    if (enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }

    EncodingType encodingType = parseEnum(EncodingType.class, config.getEncodingType());

    if (config.isBatching()) {
      if (config.getBatchSize() <= 0) {
        throw new IllegalArgumentException(
            String.format(
                "Batch size must be positive when batching is enabled, but found: %d. "
                    + "If omitted, ensure default is applied correctly.",
                config.getBatchSize()));
      }
      if (encodingType == EncodingType.JSON_OBJECT) {
        throw new IllegalArgumentException(
            "JSON_OBJECT encoding does not support batching. Use JSON_OBJECT_LINE or JSON_ARRAY instead.");
      }
    }

    if (encodingType == EncodingType.PARQUET) {
      if (!config.isBatching()) {
        throw new IllegalArgumentException("PARQUET encoding requires batching mode to be enabled");
      }
      if (config.getAvroSchema() == null || config.getAvroSchema().isEmpty()) {
        throw new IllegalArgumentException("avroSchema is required for PARQUET encoding type");
      }
    }
  }
}
