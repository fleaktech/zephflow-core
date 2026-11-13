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
package io.fleak.zephflow.lib.commands.smtpsink;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.api.ConfigurationValidationException.requireNonNull;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class SmtpSinkConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SmtpSinkDto.Config config = (SmtpSinkDto.Config) commandConfig;

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
    require(
        StringUtils.isNotBlank(config.getFromAddress()),
        of("fromAddress"),
        "fromAddress is required and must not be blank.",
        "no fromAddress is provided");
    require(
        StringUtils.isNotBlank(config.getToTemplate()),
        of("toTemplate"),
        "toTemplate is required and must not be blank.",
        "no toTemplate is provided");
    require(
        StringUtils.isNotBlank(config.getSubjectTemplate()),
        of("subjectTemplate"),
        "subjectTemplate is required and must not be blank.",
        "no subjectTemplate is provided");
    require(
        StringUtils.isNotBlank(config.getBodyTemplate()),
        of("bodyTemplate"),
        "bodyTemplate is required and must not be blank.",
        "no bodyTemplate is provided");
  }
}
