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
package io.fleak.zephflow.lib.commands.kinesis;

import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.JsonConfigParser;

/** Created by bolei on 9/4/24 */
public class KinesisSinkCommandFactory extends CommandFactory {
  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    JsonConfigParser<KinesisSinkDto.Config> configParser =
        new JsonConfigParser<>(KinesisSinkDto.Config.class);
    KinesisSinkConfigValidator validator = new KinesisSinkConfigValidator();
    AwsClientFactory awsClientFactory = new AwsClientFactory();
    KinesisSinkCommandInitializerFactory initializerFactory =
        new KinesisSinkCommandInitializerFactory(awsClientFactory);
    return new KinesisSinkCommand(nodeId, jobContext, configParser, validator, initializerFactory);
  }

  @Override
  public CommandType commandType() {
    return CommandType.SINK;
  }
}
