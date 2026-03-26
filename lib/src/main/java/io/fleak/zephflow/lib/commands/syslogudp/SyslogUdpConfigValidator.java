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

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import java.nio.charset.Charset;
import org.apache.commons.lang3.StringUtils;

public class SyslogUdpConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SyslogUdpDto.Config config = (SyslogUdpDto.Config) commandConfig;

    Preconditions.checkArgument(StringUtils.isNotBlank(config.getHost()), "host must not be blank");
    Preconditions.checkArgument(
        config.getPort() > 0 && config.getPort() <= 65535,
        "port must be between 1 and 65535, got: %s",
        config.getPort());
    Preconditions.checkArgument(
        config.getBufferSize() > 0, "bufferSize must be positive, got: %s", config.getBufferSize());
    Preconditions.checkArgument(
        config.getQueueCapacity() > 0,
        "queueCapacity must be positive, got: %s",
        config.getQueueCapacity());
    try {
      Charset.forName(config.getEncoding());
    } catch (Exception e) {
      throw new IllegalArgumentException("invalid encoding: " + config.getEncoding(), e);
    }
  }
}
