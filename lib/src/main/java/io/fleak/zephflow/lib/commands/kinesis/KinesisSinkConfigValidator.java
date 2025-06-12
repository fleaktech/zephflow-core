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
package io.fleak.zephflow.lib.commands.kinesis;

import static io.fleak.zephflow.lib.utils.MiscUtils.enforceCredentials;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import org.apache.commons.lang3.StringUtils;

/** Created by bolei on 9/3/24 */
public class KinesisSinkConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    KinesisSinkDto.Config config = (KinesisSinkDto.Config) commandConfig;
    if (enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
    if (StringUtils.trimToNull(config.getPartitionKeyFieldExpressionStr()) != null) {
      PathExpression.fromString(config.getPartitionKeyFieldExpressionStr());
    }
  }
}
