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
package io.fleak.zephflow.runner;

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_ENV;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_SERVICE;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.commands.source.SimpleSourceCommand;
import io.fleak.zephflow.runner.dag.Dag;
import io.fleak.zephflow.runner.dag.Edge;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

/** Created by bolei on 2/28/25 */
public class DagExecutor {
  private final JobConfig jobConfig;
  private final DagCompiler dagCompiler;
  private final MetricClientProvider metricClientProvider;

  public static DagExecutor createDagExecutor(
      JobConfig jobConfig, MetricClientProvider metricClientProvider) {
    return createDagExecutor(
        jobConfig, OperatorCommandRegistry.OPERATOR_COMMANDS, metricClientProvider);
  }

  public static DagExecutor createDagExecutor(
      JobConfig jobConfig,
      Map<String, CommandFactory> commandFactoryMap,
      MetricClientProvider metricClientProvider) {
    DagCompiler compiler = new DagCompiler(commandFactoryMap);
    return new DagExecutor(jobConfig, compiler, metricClientProvider);
  }

  private DagExecutor(
      JobConfig jobConfig, DagCompiler compiler, MetricClientProvider metricClientProvider) {
    this.jobConfig = jobConfig;
    this.dagCompiler = compiler;
    this.metricClientProvider = metricClientProvider;
  }

  public void executeDag() throws Exception {
    var compiledDag = dagCompiler.compile(jobConfig.getDagDefinition(), true);
    Pair<Dag<OperatorCommand>, Dag<OperatorCommand>> entryNodesDagAndRest =
        Dag.splitEntryNodesAndRest(compiledDag);
    Preconditions.checkArgument(
        CollectionUtils.size(entryNodesDagAndRest.getKey().getNodes()) == 1,
        "dag executor only supports dag with exactly one entry node");
    OperatorCommand command =
        new ArrayList<>(entryNodesDagAndRest.getKey().getNodes()).getFirst().getNodeContent();
    Preconditions.checkArgument(command instanceof SourceCommand);
    SimpleSourceCommand<?> sourceCommand = (SimpleSourceCommand<?>) command;
    List<Edge> edgesFromSource = new ArrayList<>(entryNodesDagAndRest.getKey().getEdges());
    Dag<OperatorCommand> subDagWithoutSource = entryNodesDagAndRest.getValue();
    DagRunCounters counters = createCounters();
    NoSourceDagRunner noSourceDagRunner =
        new NoSourceDagRunner(
            edgesFromSource,
            subDagWithoutSource,
            metricClientProvider,
            counters,
            jobConfig.getDagDefinition().getJobContext().getDlqConfig() != null);
    try {

      sourceCommand.execute(
          jobConfig.getJobId(),
          metricClientProvider,
          new SourceEventAcceptor() {
            @Override
            public void terminate() {
              noSourceDagRunner.terminate();
            }

            @Override
            public void accept(List<RecordFleakData> recordFleakData) {
              noSourceDagRunner.run(
                  recordFleakData,
                  jobConfig.getJobId(),
                  new NoSourceDagRunner.DagRunConfig(false, false));
            }
          });
    } finally {
      sourceCommand.terminate();
    }
  }

  private DagRunCounters createCounters() {
    Map<String, String> basicTags =
        Map.of(
            METRIC_TAG_SERVICE, jobConfig.getService(),
            METRIC_TAG_ENV, jobConfig.getEnvironment());
    Map<String, String> metricTags = new HashMap<>(basicTags);
    JobContext jobContext = jobConfig.getDagDefinition().getJobContext();
    if (jobContext != null && jobContext.getMetricTags() != null) {
      metricTags.putAll(jobContext.getMetricTags());
      jobContext.setMetricTags(metricTags);
    }
    return DagRunCounters.createPipelineCounters(metricClientProvider, metricTags);
  }
}
