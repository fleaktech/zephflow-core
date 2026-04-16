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
package io.fleak.zephflow.lib.commands.jdbcsink;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class JdbcSinkConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    JdbcSinkDto.Config config = (JdbcSinkDto.Config) commandConfig;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getJdbcUrl()), "jdbcUrl must be provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getTableName()), "tableName must be provided");
    Preconditions.checkNotNull(config.getWriteMode(), "writeMode must be provided");

    if (config.getWriteMode() == JdbcSinkDto.WriteMode.UPSERT) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(config.getUpsertKeyColumns()),
          "upsertKeyColumns must be provided when writeMode is UPSERT");
    }

    Preconditions.checkArgument(
        config.getBatchSize() > 0, "batchSize must be positive, got: %s", config.getBatchSize());
  }
}
