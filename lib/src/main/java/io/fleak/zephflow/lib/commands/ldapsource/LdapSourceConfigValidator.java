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
package io.fleak.zephflow.lib.commands.ldapsource;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class LdapSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    LdapSourceDto.Config config = (LdapSourceDto.Config) commandConfig;

    Preconditions.checkArgument(StringUtils.isNotBlank(config.getLdapUrl()), "ldapUrl is required");

    String url = config.getLdapUrl().toLowerCase();
    Preconditions.checkArgument(
        url.startsWith("ldap://") || url.startsWith("ldaps://"),
        "ldapUrl must start with ldap:// or ldaps://: %s",
        config.getLdapUrl());

    Preconditions.checkArgument(StringUtils.isNotBlank(config.getBaseDn()), "baseDn is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getSearchFilter()), "searchFilter is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getCredentialId()), "credentialId is required");

    if (config.getPageSize() != null) {
      Preconditions.checkArgument(
          config.getPageSize() > 0, "pageSize must be positive, got: %s", config.getPageSize());
    }

    if (config.getTimeLimitMs() != null) {
      Preconditions.checkArgument(
          config.getTimeLimitMs() > 0,
          "timeLimitMs must be positive, got: %s",
          config.getTimeLimitMs());
    }
  }
}
