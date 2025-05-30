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
package io.fleak.zephflow.clistarter;

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.runner.DagExecutor;
import io.fleak.zephflow.runner.JobConfig;
import org.apache.commons.cli.ParseException;

/** Created by bolei on 9/26/24 */
public class Main {

  public static void main(String[] args) throws Exception {
    try {
      JobConfig jobConfig = JobCliParser.parseArgs(args);

      // TODO
      MetricClientProvider metricClientProvider =
          new MetricClientProvider.NoopMetricClientProvider();

      DagExecutor dagExecutor = DagExecutor.createDagExecutor(jobConfig, metricClientProvider);
      dagExecutor.executeDag();
    } catch (ParseException cliParseException) {
      JobCliParser.printUsage("pipelinejob");
      System.exit(1);
    }
  }
}
