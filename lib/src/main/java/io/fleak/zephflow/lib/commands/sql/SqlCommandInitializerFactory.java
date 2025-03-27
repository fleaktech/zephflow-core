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
package io.fleak.zephflow.lib.commands.sql;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;

/** Created by bolei on 9/4/24 */
public class SqlCommandInitializerFactory extends CommandInitializerFactory {
  @Override
  protected SqlCommandPartsFactory createCommandPartsFactory(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    return new SqlCommandPartsFactory(metricClientProvider);
  }

  @Override
  protected SqlCommandInitializer doCreateCommandInitializer(
      String nodeId, CommandPartsFactory commandPartsFactory) {
    return new SqlCommandInitializer(nodeId, (SqlCommandPartsFactory) commandPartsFactory);
  }
}
