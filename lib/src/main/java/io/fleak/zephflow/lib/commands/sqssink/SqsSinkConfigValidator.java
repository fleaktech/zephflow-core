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
package io.fleak.zephflow.lib.commands.sqssink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import org.apache.commons.lang3.StringUtils;

public class SqsSinkConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SqsSinkDto.Config config = (SqsSinkDto.Config) commandConfig;
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getQueueUrl()), "no queue URL is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getRegionStr()), "no region is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getEncodingType()), "no encoding type is provided");

    EncodingType encodingType = parseEnum(EncodingType.class, config.getEncodingType());
    SerializerFactory.validateEncodingType(encodingType);

    if (config.getBatchSize() != null) {
      Preconditions.checkArgument(
          config.getBatchSize() >= 1 && config.getBatchSize() <= SqsSinkDto.MAX_BATCH_SIZE,
          "batchSize must be between 1 and %s",
          SqsSinkDto.MAX_BATCH_SIZE);
    }

    boolean isFifoQueue = config.getQueueUrl().endsWith(".fifo");
    if (isFifoQueue) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(config.getMessageGroupIdExpression()),
          "messageGroupIdExpression is required for FIFO queues");
    }

    if (StringUtils.isNotBlank(config.getMessageGroupIdExpression())) {
      PathExpression.fromString(config.getMessageGroupIdExpression());
    }
    if (StringUtils.isNotBlank(config.getDeduplicationIdExpression())) {
      PathExpression.fromString(config.getDeduplicationIdExpression());
    }

    if (StringUtils.trimToNull(config.getCredentialId()) != null
        && enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
