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
package io.fleak.zephflow.lib.commands.azureeventhubsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.parseEnum;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubConfigValidation;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import org.apache.commons.lang3.StringUtils;

/** Validates {@link AzureEventHubSinkDto.Config}. */
public class AzureEventHubSinkConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    AzureEventHubSinkDto.Config config = (AzureEventHubSinkDto.Config) commandConfig;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getEventHubName()), "no eventHubName is provided");

    AzureEventHubConfigValidation.validateEventHubAuth(
        config.getConnectionString(),
        config.getFullyQualifiedNamespace(),
        config.getTenantId(),
        config.getClientId(),
        config.getClientSecret());

    EncodingType encodingType = parseEnum(EncodingType.class, config.getEncodingType());
    SerializerFactory.validateEncodingType(encodingType);

    if (StringUtils.isNotBlank(config.getPartitionKeyFieldExpressionStr())) {
      PathExpression.fromString(config.getPartitionKeyFieldExpressionStr());
    }
  }
}
