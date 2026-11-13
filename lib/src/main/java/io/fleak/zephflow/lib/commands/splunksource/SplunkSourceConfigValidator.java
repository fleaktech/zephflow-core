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
package io.fleak.zephflow.lib.commands.splunksource;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.JobContext;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.commons.lang3.StringUtils;

public class SplunkSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SplunkSourceDto.Config config = (SplunkSourceDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getSplunkUrl()),
        of("splunkUrl"),
        "splunkUrl is required and must not be blank.",
        "splunkUrl is required");
    require(
        StringUtils.isNotBlank(config.getSearchQuery()),
        of("searchQuery"),
        "searchQuery is required and must not be blank.",
        "searchQuery is required");
    require(
        StringUtils.isNotBlank(config.getCredentialId()),
        of("credentialId"),
        "credentialId is required and must not be blank.",
        "credentialId is required");

    try {
      URL url = new URL(config.getSplunkUrl());
      require(
          StringUtils.isNotBlank(url.getHost()),
          of("splunkUrl"),
          "splunkUrl must include a host.",
          "splunkUrl must have a host: %s",
          config.getSplunkUrl());
      String scheme = url.getProtocol();
      require(
          "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme),
          of("splunkUrl"),
          "splunkUrl must use the http or https scheme.",
          "splunkUrl must have http or https scheme: %s",
          config.getSplunkUrl());
    } catch (MalformedURLException e) {
      throw ConfigurationValidationException.at(
          of("splunkUrl"),
          new IllegalArgumentException("splunkUrl is not a valid URL: " + config.getSplunkUrl(), e),
          "splunkUrl must be a valid http or https URL with a host.");
    }

    require(
        config.getJobInitTimeoutMs() >= 0,
        of("jobInitTimeoutMs"),
        "jobInitTimeoutMs must be greater than or equal to 0.",
        "jobInitTimeoutMs must be non-negative");
  }
}
