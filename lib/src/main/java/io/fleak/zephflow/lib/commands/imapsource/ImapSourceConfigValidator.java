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
package io.fleak.zephflow.lib.commands.imapsource;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.api.ConfigurationValidationException.requireNonNull;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.JobContext;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class ImapSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    ImapSourceDto.Config config = (ImapSourceDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getHost()),
        of("host"),
        "host is required and must not be blank.",
        "no host is provided");
    requireNonNull(config.getPort(), of("port"), "port is required.", "no port is provided");
    require(
        config.getPort() > 0,
        of("port"),
        "port must be greater than 0.",
        "port must be positive, got: %s",
        config.getPort());
    require(
        StringUtils.isNotBlank(config.getCredentialId()),
        of("credentialId"),
        "credentialId is required and must not be blank.",
        "no credentialId is provided");
    requireNonNull(
        config.getAuthType(), of("authType"), "authType is required.", "no authType is provided");

    if (config.getMaxMessages() != null) {
      require(
          config.getMaxMessages() > 0,
          of("maxMessages"),
          "maxMessages must be greater than 0.",
          "maxMessages must be positive, got: %s",
          config.getMaxMessages());
    }

    if (config.getPollIntervalMs() != null) {
      require(
          config.getPollIntervalMs() > 0,
          of("pollIntervalMs"),
          "pollIntervalMs must be greater than 0.",
          "pollIntervalMs must be positive, got: %s",
          config.getPollIntervalMs());
    }

    // markAsRead=true makes the fetcher AND the user criteria with `\Seen=false` to keep
    // de-duplication closed across polls; combining that with searchCriteria=SEEN would always
    // produce an empty result set, which is almost certainly a misconfiguration.
    if (Boolean.TRUE.equals(config.getMarkAsRead())
        && "SEEN".equalsIgnoreCase(StringUtils.trimToEmpty(config.getSearchCriteria()))) {
      var original =
          new IllegalArgumentException(
              "searchCriteria=SEEN is incompatible with markAsRead=true (the effective filter becomes"
                  + " SEEN AND UNSEEN, which matches nothing). Set markAsRead=false or pick a"
                  + " different searchCriteria.");
      throw new ConfigurationValidationException(
          original,
          List.of("searchCriteria", "markAsRead").stream()
              .map(
                  field ->
                      new ConfigurationIssue(
                          of(field),
                          "searchCriteria SEEN is incompatible with markAsRead true; set markAsRead false or use different searchCriteria."))
              .toList());
    }
  }
}
