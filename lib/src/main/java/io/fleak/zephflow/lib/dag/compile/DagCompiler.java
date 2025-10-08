package io.fleak.zephflow.lib.dag.compile;

import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.lib.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.lib.dag.Dag;
import java.util.Map;

/** Created by bolei on 10/8/25 */
public abstract class DagCompiler {
  protected final Map<String, CommandFactory> commandFactoryMap;

  protected DagCompiler(Map<String, CommandFactory> commandFactoryMap) {
    this.commandFactoryMap = commandFactoryMap;
  }

  public abstract Dag<OperatorCommand> compile(
      AdjacencyListDagDefinition adjacencyListDagDefinition, boolean checkConnected);
}
