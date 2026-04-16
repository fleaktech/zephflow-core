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
package io.fleak.zephflow.lib.commands.gcssource;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.source.SourceCommandFactory;
import io.fleak.zephflow.lib.gcp.GcsClientFactory;

public class GcsSourceCommandFactory extends SourceCommandFactory {
  @Override
  public GcsSourceCommand createCommand(String nodeId, JobContext jobContext) {
    return new GcsSourceCommand(
        nodeId,
        jobContext,
        new JsonConfigParser<>(GcsSourceDto.Config.class),
        new GcsSourceConfigValidator(),
        new GcsClientFactory());
  }
}
