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
package io.fleak.zephflow.lib.commands.influxdbsink;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.JobContext;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/** Validates {@link InfluxDbSinkDto.Config}. */
public class InfluxDbSinkConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    InfluxDbSinkDto.Config config = (InfluxDbSinkDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getUrl()),
        of("url"),
        "url is required and must not be blank.",
        "url is required");
    require(
        StringUtils.isNotBlank(config.getOrg()),
        of("org"),
        "org is required and must not be blank.",
        "org is required");
    require(
        StringUtils.isNotBlank(config.getBucket()),
        of("bucket"),
        "bucket is required and must not be blank.",
        "bucket is required");

    boolean hasMeasurement = StringUtils.isNotBlank(config.getMeasurement());
    boolean hasMeasurementField = StringUtils.isNotBlank(config.getMeasurementField());
    if (!(hasMeasurement ^ hasMeasurementField)) {
      throw new ConfigurationValidationException(
          new IllegalArgumentException("exactly one of measurement / measurementField must be set"),
          List.of("measurement", "measurementField").stream()
              .map(
                  field ->
                      new ConfigurationIssue(
                          of(field), "exactly one of measurement / measurementField must be set"))
              .toList());
    }

    if (config.getBatchSize() != null) {
      require(
          config.getBatchSize() >= 1,
          of("batchSize"),
          "batchSize must be at least 1.",
          "batchSize must be at least 1");
    }

    if (StringUtils.isNotBlank(config.getCredentialId()) && enforceCredentials(jobContext)) {
      lookupApiKeyCredential(jobContext, config.getCredentialId());
    }
  }
}
