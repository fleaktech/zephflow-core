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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.sink.SinkEncoding;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

public class SplunkHecSinkConfigValidator implements ConfigValidator {

  public static final WriterCapability CAPABILITY =
      new WriterCapability(
          COMMAND_NAME_SPLUNK_HEC_SINK,
          Unit.ONE_RECORD,
          Set.of("json", "csv", "kv", "text"),
          Set.of(JsonFormatConfig.Mode.OBJECT),
          true,
          Set.of());

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SplunkHecSinkDto.Config config = (SplunkHecSinkDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getHecUrl()),
        of("hecUrl"),
        "hecUrl is required and must not be blank.",
        "hecUrl is required");
    URI uri;
    try {
      uri = new URI(config.getHecUrl());
    } catch (URISyntaxException e) {
      throw ConfigurationValidationException.at(
          of("hecUrl"),
          new IllegalArgumentException("hecUrl is not a valid URI: " + config.getHecUrl(), e),
          "Invalid URI: " + e.getReason() + " at index " + e.getIndex() + ".");
    }
    String scheme = uri.getScheme();
    require(
        "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme),
        of("hecUrl"),
        "hecUrl must use the http or https scheme.",
        "hecUrl must use http or https scheme");
    require(
        StringUtils.isNotBlank(uri.getHost()),
        of("hecUrl"),
        "hecUrl must include a host.",
        "hecUrl must include a host");

    require(
        StringUtils.isNotBlank(config.getCredentialId()),
        of("credentialId"),
        "credentialId is required and must not be blank.",
        "credentialId is required");

    if (config.getBatchSize() != null) {
      require(
          config.getBatchSize() >= 1,
          of("batchSize"),
          "batchSize must be at least 1.",
          "batchSize must be at least 1");
    }

    if (config.getEncoding() != null) {
      SinkEncoding.validate(config.getEncoding(), CAPABILITY);
    }

    if (config.isStoreAndForwardEnabled()) {
      require(
          config.getLocalStoreMaxBytes() > 0,
          of("localStoreMaxBytes"),
          "localStoreMaxBytes must be greater than 0 when storeAndForwardEnabled is true.",
          "localStoreMaxBytes must be positive when storeAndForwardEnabled. Got: %s",
          config.getLocalStoreMaxBytes());
      require(
          config.getForwardRetryIntervalMillis() > 0,
          of("forwardRetryIntervalMillis"),
          "forwardRetryIntervalMillis must be greater than 0 when storeAndForwardEnabled is true.",
          "forwardRetryIntervalMillis must be positive when storeAndForwardEnabled. Got: %s",
          config.getForwardRetryIntervalMillis());
    }

    if (enforceCredentials(jobContext)) {
      lookupApiKeyCredential(jobContext, config.getCredentialId());
    }
  }
}
