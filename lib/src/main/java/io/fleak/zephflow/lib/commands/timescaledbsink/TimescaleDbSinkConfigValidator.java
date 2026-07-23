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
package io.fleak.zephflow.lib.commands.timescaledbsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.jdbcsink.JdbcSinkDto;
import org.apache.commons.lang3.StringUtils;

public class TimescaleDbSinkConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    TimescaleDbSinkDto.Config config = (TimescaleDbSinkDto.Config) commandConfig;

    Preconditions.checkArgument(StringUtils.isNotBlank(config.getJdbcUrl()), "jdbcUrl is required");
    Preconditions.checkArgument(
        config.getJdbcUrl().startsWith("jdbc:postgresql:"),
        "jdbcUrl must be a PostgreSQL URL (jdbc:postgresql:...); TimescaleDB is a PostgreSQL extension");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getTableName()), "tableName is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getTimeColumn()), "timeColumn is required");
    Preconditions.checkArgument(config.getTimeUnit() != null, "timeUnit is required");
    Preconditions.checkArgument(config.getWriteMode() != null, "writeMode is required");

    if (config.getWriteMode() == JdbcSinkDto.WriteMode.UPSERT) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(config.getUpsertKeyColumns()),
          "upsertKeyColumns is required when writeMode is UPSERT");
    }

    Preconditions.checkArgument(config.getBatchSize() > 0, "batchSize must be at least 1");

    if (StringUtils.isNotBlank(config.getCredentialId()) && enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
