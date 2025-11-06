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
package io.fleak.zephflow.sparkrunner;

import static io.fleak.zephflow.lib.utils.YamlUtils.fromYamlResource;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.lib.dag.Dag;
import io.fleak.zephflow.lib.dag.Edge;
import io.fleak.zephflow.runner.NoSourceDagRunner;
import io.fleak.zephflow.runner.ZephflowDagCompiler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/** Test utility for creating DAG runners in tests. */
public class TestDagRunnerUtil {

  /**
   * Creates a NoSourceDagRunner from test_dag.yml with real DAG nodes and test factories.
   *
   * @return Configured NoSourceDagRunner for testing
   * @throws Exception if DAG loading or compilation fails
   */
  public static NoSourceDagRunner createRealDagRunner() throws Exception {
    // Load and compile test_dag.yml
    AdjacencyListDagDefinition dagDef =
        fromYamlResource("/test_dag.yml", new TypeReference<>() {});

    Map<String, CommandFactory> commandFactoryMap =
        new HashMap<>(OperatorCommandRegistry.OPERATOR_COMMANDS);
    commandFactoryMap.put(
        "testSource", new io.fleak.zephflow.runner.DagExecutorTest.TestSourceFactory());
    commandFactoryMap.put(
        "testSink", new io.fleak.zephflow.runner.DagExecutorTest.TestSinkFactory());

    ZephflowDagCompiler compiler = new ZephflowDagCompiler(commandFactoryMap);
    Dag<OperatorCommand> compiledDag = compiler.compile(dagDef, true);

    // Split off source node
    Pair<Dag<OperatorCommand>, Dag<OperatorCommand>> split =
        Dag.splitEntryNodesAndRest(compiledDag);

    // Get edges from source
    List<Edge> edgesFromSource = split.getKey().getEdges();

    // Create JobContext with required metric tags
    JobContext jobContext =
        JobContext.builder()
            .metricTags(Map.of("service", "test-service", "env", "test-env"))
            .build();

    // Create runner with DAG (excluding source)
    return new NoSourceDagRunner(edgesFromSource, split.getValue(), jobContext);
  }

  private TestDagRunnerUtil() {
    // Utility class
  }
}
