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

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class SmtpSinkConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SmtpSinkDto.Config config = (SmtpSinkDto.Config) commandConfig;

    Preconditions.checkArgument(StringUtils.isNotBlank(config.getHost()), "no host is provided");
    Preconditions.checkNotNull(config.getPort(), "no port is provided");
    Preconditions.checkArgument(
        config.getPort() > 0, "port must be positive, got: %s", config.getPort());
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getCredentialId()), "no credentialId is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getFromAddress()), "no fromAddress is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getToTemplate()), "no toTemplate is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getSubjectTemplate()), "no subjectTemplate is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getBodyTemplate()), "no bodyTemplate is provided");
  }
}
