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

import static io.fleak.zephflow.lib.utils.MiscUtils.enforceCredentials;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;
import software.amazon.kinesis.common.InitialPositionInStream;

public class KinesisSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    KinesisSourceDto.Config config = (KinesisSourceDto.Config) commandConfig;
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getApplicationName()), "no application name is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getRegionStr()), "no region name is provided");
    Preconditions.checkNotNull(config.getEncodingType(), "no encoding type is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getStreamName()), "no stream name is provided");

    var initialPosition = config.getInitialPosition();
    var initialTS = config.getInitialPositionTimestamp();

    if (initialPosition != null) {
      if (initialPosition.equals(InitialPositionInStream.AT_TIMESTAMP)) {
        Preconditions.checkState(
            initialTS != null,
            "If initial position is AT_TIMESTAMP then a timestamp must be provided");
      } else {
        Preconditions.checkState(
            initialTS == null,
            "Timestamp must not be provided unless initial position is AT_TIMESTAMP. Found: %s with timestamp %s",
            initialPosition,
            initialTS);
      }
    } else {
      Preconditions.checkState(
          initialTS == null,
          "Timestamp must not be provided unless initial position is AT_TIMESTAMP");
    }
    if (StringUtils.trimToNull(config.getCredentialId()) != null
        && enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
