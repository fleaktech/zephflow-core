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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class LdapSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    LdapSourceDto.Config config = (LdapSourceDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getLdapUrl()),
        of("ldapUrl"),
        "ldapUrl is required and must not be blank.",
        "ldapUrl is required");

    String url = config.getLdapUrl().toLowerCase();
    require(
        url.startsWith("ldap://") || url.startsWith("ldaps://"),
        of("ldapUrl"),
        "ldapUrl must start with ldap:// or ldaps://.",
        "ldapUrl must start with ldap:// or ldaps://: %s",
        config.getLdapUrl());

    require(
        StringUtils.isNotBlank(config.getBaseDn()),
        of("baseDn"),
        "baseDn is required and must not be blank.",
        "baseDn is required");
    require(
        StringUtils.isNotBlank(config.getSearchFilter()),
        of("searchFilter"),
        "searchFilter is required and must not be blank.",
        "searchFilter is required");
    require(
        StringUtils.isNotBlank(config.getCredentialId()),
        of("credentialId"),
        "credentialId is required and must not be blank.",
        "credentialId is required");

    if (config.getPageSize() != null) {
      require(
          config.getPageSize() > 0,
          of("pageSize"),
          "pageSize must be greater than 0.",
          "pageSize must be positive, got: %s",
          config.getPageSize());
    }

    if (config.getTimeLimitMs() != null) {
      require(
          config.getTimeLimitMs() > 0,
          of("timeLimitMs"),
          "timeLimitMs must be greater than 0.",
          "timeLimitMs must be positive, got: %s",
          config.getTimeLimitMs());
    }
  }
}
