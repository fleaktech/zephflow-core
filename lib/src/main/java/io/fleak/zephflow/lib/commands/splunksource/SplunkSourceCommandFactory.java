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
package io.fleak.zephflow.lib.commands.splunksource;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.source.SourceCommandFactory;

public class SplunkSourceCommandFactory extends SourceCommandFactory {
  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    JsonConfigParser<SplunkSourceDto.Config> configParser =
        new JsonConfigParser<>(SplunkSourceDto.Config.class);
    SplunkSourceConfigValidator validator = new SplunkSourceConfigValidator();
    return new SplunkSourceCommand(nodeId, jobContext, configParser, validator);
  }
}
