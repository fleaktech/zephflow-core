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
import io.fleak.zephflow.lib.dag.Dag;
import io.fleak.zephflow.lib.dag.Edge;
import io.fleak.zephflow.lib.dag.compile.CommandProvider;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

/** Created by bolei on 2/28/25 */
@Slf4j
public record DagExecutor(
    JobConfig jobConfig,
    ZephflowDagCompiler zephflowDagCompiler,
    MetricClientProvider metricClientProvider) {
  public static DagExecutor createDagExecutor(
      JobConfig jobConfig, MetricClientProvider metricClientProvider) {

    // --- SPI Discovery and Command Aggregation ---

    Map<String, CommandFactory> aggregatedCommands = loadCommands();
    return createDagExecutor(jobConfig, aggregatedCommands, metricClientProvider);
  }

  public static Map<String, CommandFactory> loadCommands() {
    Map<String, CommandFactory> aggregatedCommands = new HashMap<>();
    ServiceLoader<CommandProvider> loader = ServiceLoader.load(CommandProvider.class);

    log.info("Discovering command providers...");
    for (CommandProvider provider : loader) {
      String providerName = provider.getClass().getName();
      log.info("Loading commands from provider: {}", providerName);
      try {
        Map<String, CommandFactory> providerCommands = provider.getCommands();
        if (providerCommands == null) {
          continue;
        }
        providerCommands.forEach(
            (key, value) -> {
              if (aggregatedCommands.containsKey(key)) {
                throw new IllegalStateException("Duplicate command detected: " + key);
              }
              log.info("Loading command: {}", key);
              aggregatedCommands.put(key, value);
            });

      } catch (Exception e) {
        log.error("Failed to load commands from provider {}: {}", providerName, e.getMessage());
        throw e;
      }
    }

    if (aggregatedCommands.isEmpty()) {
      System.err.println(
          "Warning: No commands were discovered. Check classpath and META-INF/services configuration.");
      System.exit(1);
    }
    return aggregatedCommands;
  }

  public static DagExecutor createDagExecutor(
      JobConfig jobConfig,
      Map<String, CommandFactory> commandFactoryMap,
      MetricClientProvider metricClientProvider) {
    ZephflowDagCompiler compiler = new ZephflowDagCompiler(commandFactoryMap);
    return new DagExecutor(jobConfig, compiler, metricClientProvider);
  }

  public void executeDag() throws Exception {
    var compiledDag = zephflowDagCompiler.compile(jobConfig.getDagDefinition(), true);
    Pair<Dag<OperatorCommand>, Dag<OperatorCommand>> entryNodesDagAndRest =
        Dag.splitEntryNodesAndRest(compiledDag);
    Preconditions.checkArgument(
        CollectionUtils.size(entryNodesDagAndRest.getKey().getNodes()) == 1,
        "dag executor only supports dag with exactly one entry node");
    OperatorCommand command =
        new ArrayList<>(entryNodesDagAndRest.getKey().getNodes()).get(0).getNodeContent();
    Preconditions.checkArgument(command instanceof SourceCommand);
    SourceCommand sourceCommand = (SourceCommand) command;
    List<Edge> edgesFromSource = new ArrayList<>(entryNodesDagAndRest.getKey().getEdges());
    Dag<OperatorCommand> subDagWithoutSource = entryNodesDagAndRest.getValue();
    DagRunCounters counters = createCounters();
    NoSourceDagRunner noSourceDagRunner =
        new NoSourceDagRunner(
            edgesFromSource,
            subDagWithoutSource,
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
                  new NoSourceDagRunner.DagRunConfig(true, false),
                  metricClientProvider,
                  counters);
            }
          });
    } finally {
      sourceCommand.terminate();
      noSourceDagRunner.terminate();
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
