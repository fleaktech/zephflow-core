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
package io.fleak.zephflow.lib.commands.syslogudp;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.JobContext;
import java.nio.charset.Charset;
import org.apache.commons.lang3.StringUtils;

public class SyslogUdpConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SyslogUdpDto.Config config = (SyslogUdpDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getHost()),
        of("host"),
        "host is required and must not be blank.",
        "host must not be blank");
    require(
        config.getPort() > 0 && config.getPort() <= 65535,
        of("port"),
        "port must be between 1 and 65535.",
        "port must be between 1 and 65535, got: %s",
        config.getPort());
    require(
        config.getBufferSize() > 0,
        of("bufferSize"),
        "bufferSize must be greater than 0.",
        "bufferSize must be positive, got: %s",
        config.getBufferSize());
    require(
        config.getQueueCapacity() > 0,
        of("queueCapacity"),
        "queueCapacity must be greater than 0.",
        "queueCapacity must be positive, got: %s",
        config.getQueueCapacity());
    try {
      Charset.forName(config.getEncoding());
    } catch (Exception e) {
      throw ConfigurationValidationException.at(
          of("encoding"),
          new IllegalArgumentException("invalid encoding: " + config.getEncoding(), e),
          "encoding must name a supported charset, such as UTF-8.");
    }
  }
}
