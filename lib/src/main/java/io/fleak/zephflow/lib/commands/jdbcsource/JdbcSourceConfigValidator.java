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
package io.fleak.zephflow.lib.commands.jdbcsource;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class JdbcSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    JdbcSourceDto.Config config = (JdbcSourceDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getJdbcUrl()),
        of("jdbcUrl"),
        "jdbcUrl is required and must not be blank.",
        "jdbcUrl must be provided");
    require(
        StringUtils.isNotBlank(config.getQuery()),
        of("query"),
        "query is required and must not be blank.",
        "query must be provided");

    if (StringUtils.isNotBlank(config.getWatermarkColumn())) {
      require(
          config.getQuery().contains(":watermark"),
          of("query"),
          "query must contain the :watermark placeholder when watermarkColumn is set.",
          "query must contain :watermark placeholder when watermarkColumn is set");
    }

    require(
        config.getPollIntervalMs() > 0,
        of("pollIntervalMs"),
        "pollIntervalMs must be greater than 0.",
        "pollIntervalMs must be positive, got: %s",
        config.getPollIntervalMs());
    require(
        config.getFetchSize() > 0,
        of("fetchSize"),
        "fetchSize must be greater than 0.",
        "fetchSize must be positive, got: %s",
        config.getFetchSize());
  }
}
