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

import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.runner.dag.*;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Created by bolei on 3/4/25 */
@Slf4j
public class DagCompiler {
  private final Map<String, CommandFactory> commandFactoryMap;

  public DagCompiler(Map<String, CommandFactory> commandFactoryMap) {
    this.commandFactoryMap = commandFactoryMap;
  }

  public Dag<OperatorCommand> compile(
      AdjacencyListDagDefinition adjacencyListDagDefinition, boolean checkConnected)
      throws Exception {
    NodesEdgesDagDefinition nodesEdgesDagDefinition =
        NodesEdgesDagDefinition.fromAdjacencyListDagDefinition(adjacencyListDagDefinition);
    var rawDag = nodesEdgesDagDefinition.getDag();
    var jobContext = nodesEdgesDagDefinition.getJobContext();

    Predicate<Node<RawDagNode>> rawSourceNodePredicate =
        n -> {
          String commandName = n.getNodeContent().getCommandName();
          CommandFactory commandFactory = getCommandFactory(commandName);
          return commandFactory.commandType() == CommandType.SOURCE;
        };

    Predicate<Node<RawDagNode>> rawSinkNodePredicate =
        n -> {
          String commandName = n.getNodeContent().getCommandName();
          CommandFactory commandFactory = getCommandFactory(commandName);
          return commandFactory.commandType() == CommandType.SINK;
        };

    // Validate the DAG structure
    rawDag.validate(checkConnected, rawSourceNodePredicate, rawSinkNodePredicate);

    // Compile nodes
    List<Node<OperatorCommand>> compiledNodes =
        rawDag.getNodes().stream()
            .map(
                n -> {
                  RawDagNode rdn = n.getNodeContent();
                  try {
                    CommandFactory commandFactory = getCommandFactory(rdn.getCommandName());

                    OperatorCommand command = commandFactory.createCommand(n.getId(), jobContext);
                    command.parseAndValidateArg(n.getNodeContent().getArg());
                    return Node.<OperatorCommand>builder()
                        .nodeContent(command)
                        .id(n.getId())
                        .build();
                  } catch (Exception e) {
                    log.error("dag compilation error at node {}: {}", n.getId(), rdn, e);
                    throw new IllegalArgumentException(
                        String.format(
                            "failed to compile dag at node %s: %s, reason: %s ",
                            n.getId(), rdn, e.getMessage()));
                  }
                })
            .collect(Collectors.toList());

    // Compile edges (now simpler without conditions)
    List<Edge> compiledEdges =
        rawDag.getEdges().stream()
            .map(e -> Edge.builder().from(e.getFrom()).to(e.getTo()).build())
            .collect(Collectors.toList());

    return new Dag<>(compiledNodes, compiledEdges);
  }

  private CommandFactory getCommandFactory(String commandName) {
    CommandFactory commandFactory = commandFactoryMap.get(commandName);
    if (commandFactory == null) {
      throw new IllegalArgumentException("unknown command: " + commandName);
    }
    return commandFactory;
  }
}
