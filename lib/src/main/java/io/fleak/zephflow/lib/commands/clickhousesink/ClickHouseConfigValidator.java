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
package io.fleak.zephflow.lib.commands.clickhousesink;

import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredentialOpt;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public class ClickHouseConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    var config = (ClickHouseSinkDto.Config) commandConfig;
    var usernamePasswordCredential =
        lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());
    if (config.getCredentialId() != null && !(usernamePasswordCredential.isPresent())) {
      throw new RuntimeException(
          "The credentialId is specific but no credentials record was found");
    }
    Objects.requireNonNull(
        StringUtils.trimToNull(config.getEndpoint()), "A clickhouse endpoint must be specified");
    Objects.requireNonNull(
        StringUtils.trimToNull(config.getDatabase()), "A clickhouse database must be specified");
    Objects.requireNonNull(
        StringUtils.trimToNull(config.getTable()), "A clickhouse table must be specified");
  }
}
