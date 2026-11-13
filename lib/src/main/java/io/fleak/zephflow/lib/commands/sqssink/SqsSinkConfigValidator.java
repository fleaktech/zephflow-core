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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.sink.SinkEncoding;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

public class SqsSinkConfigValidator implements ConfigValidator {

  public static final WriterCapability CAPABILITY =
      new WriterCapability(
          COMMAND_NAME_SQS_SINK,
          Unit.ONE_RECORD,
          Set.of("json"),
          Set.of(JsonFormatConfig.Mode.OBJECT),
          false,
          Set.of());

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SqsSinkDto.Config config = (SqsSinkDto.Config) commandConfig;
    require(
        StringUtils.isNotBlank(config.getQueueUrl()),
        of("queueUrl"),
        "queueUrl is required and must not be blank.",
        "no queue URL is provided");
    require(
        StringUtils.isNotBlank(config.getRegionStr()),
        of("regionStr"),
        "regionStr is required and must not be blank.",
        "no region is provided");
    if (config.getEncoding() != null) {
      SinkEncoding.validate(config.getEncoding(), CAPABILITY);
    }

    if (config.getBatchSize() != null) {
      require(
          config.getBatchSize() >= 1 && config.getBatchSize() <= SqsSinkDto.MAX_BATCH_SIZE,
          of("batchSize"),
          "batchSize must be between 1 and " + SqsSinkDto.MAX_BATCH_SIZE + ".",
          "batchSize must be between 1 and %s",
          SqsSinkDto.MAX_BATCH_SIZE);
    }

    boolean isFifoQueue = config.getQueueUrl().endsWith(".fifo");
    if (isFifoQueue) {
      require(
          StringUtils.isNotBlank(config.getMessageGroupIdExpression()),
          of("messageGroupIdExpression"),
          "messageGroupIdExpression is required and must not be blank for FIFO queues.",
          "messageGroupIdExpression is required for FIFO queues");
    }

    if (StringUtils.isNotBlank(config.getMessageGroupIdExpression())) {
      PathExpression.fromString(
          config.getMessageGroupIdExpression(), of("messageGroupIdExpression"));
    }
    if (StringUtils.isNotBlank(config.getDeduplicationIdExpression())) {
      PathExpression.fromString(
          config.getDeduplicationIdExpression(), of("deduplicationIdExpression"));
    }

    if (StringUtils.trimToNull(config.getCredentialId()) != null
        && enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
