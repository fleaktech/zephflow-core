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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.jdbcsink.JdbcSinkDto;
import org.apache.commons.lang3.StringUtils;

public class TimescaleDbSinkConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    TimescaleDbSinkDto.Config config = (TimescaleDbSinkDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getJdbcUrl()),
        of("jdbcUrl"),
        "jdbcUrl is required and must not be blank.",
        "jdbcUrl is required");
    require(
        config.getJdbcUrl().startsWith("jdbc:postgresql:"),
        of("jdbcUrl"),
        "jdbcUrl must start with jdbc:postgresql:.",
        "jdbcUrl must be a PostgreSQL URL (jdbc:postgresql:...); TimescaleDB is a PostgreSQL extension");
    require(
        StringUtils.isNotBlank(config.getTableName()),
        of("tableName"),
        "tableName is required and must not be blank.",
        "tableName is required");
    require(
        StringUtils.isNotBlank(config.getTimeColumn()),
        of("timeColumn"),
        "timeColumn is required and must not be blank.",
        "timeColumn is required");
    require(
        config.getTimeUnit() != null,
        of("timeUnit"),
        "timeUnit is required.",
        "timeUnit is required");
    require(
        config.getWriteMode() != null,
        of("writeMode"),
        "writeMode is required.",
        "writeMode is required");

    if (config.getWriteMode() == JdbcSinkDto.WriteMode.UPSERT) {
      require(
          StringUtils.isNotBlank(config.getUpsertKeyColumns()),
          of("upsertKeyColumns"),
          "upsertKeyColumns is required and must not be blank when writeMode is UPSERT.",
          "upsertKeyColumns is required when writeMode is UPSERT");
    }

    require(
        config.getBatchSize() > 0,
        of("batchSize"),
        "batchSize must be at least 1.",
        "batchSize must be at least 1");

    if (StringUtils.isNotBlank(config.getCredentialId()) && enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
