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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.lib.utils.MiscUtils.enforceCredentials;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential;

import com.google.common.base.Strings;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import org.apache.commons.lang3.StringUtils;
import software.amazon.kinesis.common.InitialPositionInStream;

public class KinesisSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    KinesisSourceDto.Config config = (KinesisSourceDto.Config) commandConfig;
    require(
        StringUtils.isNotBlank(config.getApplicationName()),
        of("applicationName"),
        "applicationName is required and must not be blank.",
        "no application name is provided");
    require(
        StringUtils.isNotBlank(config.getRegionStr()),
        of("regionStr"),
        "regionStr is required and must not be blank.",
        "no region name is provided");
    // Compiles the decoding, so a bad one fails validation rather than the first message.
    SourceDecoding.validate(KinesisSourceCommand.decodingConfig(config));
    require(
        StringUtils.isNotBlank(config.getStreamName()),
        of("streamName"),
        "streamName is required and must not be blank.",
        "no stream name is provided");

    var initialPosition = config.getInitialPosition();
    var initialTS = config.getInitialPositionTimestamp();

    if (initialPosition != null) {
      if (initialPosition.equals(InitialPositionInStream.AT_TIMESTAMP)) {
        if (initialTS == null) {
          throw ConfigurationValidationException.at(
              of("initialPositionTimestamp"),
              new IllegalStateException(
                  Strings.lenientFormat(
                      "If initial position is AT_TIMESTAMP then a timestamp must be provided")),
              "initialPositionTimestamp is required when initialPosition is AT_TIMESTAMP.");
        }
      } else {
        if (initialTS != null) {
          throw ConfigurationValidationException.at(
              of("initialPositionTimestamp"),
              new IllegalStateException(
                  Strings.lenientFormat(
                      "Timestamp must not be provided unless initial position is AT_TIMESTAMP. Found: %s with timestamp %s",
                      initialPosition, initialTS)),
              "initialPositionTimestamp must be absent unless initialPosition is AT_TIMESTAMP.");
        }
      }
    } else {
      if (initialTS != null) {
        throw ConfigurationValidationException.at(
            of("initialPositionTimestamp"),
            new IllegalStateException(
                Strings.lenientFormat(
                    "Timestamp must not be provided unless initial position is AT_TIMESTAMP")),
            "initialPositionTimestamp must be absent unless initialPosition is AT_TIMESTAMP.");
      }
    }
    if (StringUtils.trimToNull(config.getCredentialId()) != null
        && enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
