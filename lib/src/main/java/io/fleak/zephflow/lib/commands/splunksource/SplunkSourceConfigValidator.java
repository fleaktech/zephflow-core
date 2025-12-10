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

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.commons.lang3.StringUtils;

public class SplunkSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SplunkSourceDto.Config config = (SplunkSourceDto.Config) commandConfig;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getSplunkUrl()), "splunkUrl is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getSearchQuery()), "searchQuery is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getCredentialId()), "credentialId is required");

    try {
      URL url = new URL(config.getSplunkUrl());
      Preconditions.checkArgument(
          StringUtils.isNotBlank(url.getHost()),
          "splunkUrl must have a host: %s",
          config.getSplunkUrl());
      String scheme = url.getProtocol();
      Preconditions.checkArgument(
          "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme),
          "splunkUrl must have http or https scheme: %s",
          config.getSplunkUrl());
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(
          "splunkUrl is not a valid URL: " + config.getSplunkUrl(), e);
    }

    Preconditions.checkArgument(
        config.getJobInitTimeoutMs() >= 0, "jobInitTimeoutMs must be non-negative");
  }
}
