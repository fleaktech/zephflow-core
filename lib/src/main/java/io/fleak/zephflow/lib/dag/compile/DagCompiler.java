package io.fleak.zephflow.lib.dag.compile;

import io.fleak.zephflow.lib.dag.AdjacencyListDagDefinition;
import java.util.Map;

/** Created by bolei on 10/8/25 */
public abstract class DagCompiler<T, U> {
  protected final Map<String, U> commandRegistryMap;

  protected DagCompiler(Map<String, U> commandRegistryMap) {
    this.commandRegistryMap = commandRegistryMap;
  }

  public abstract T compile(
      AdjacencyListDagDefinition adjacencyListDagDefinition, boolean checkConnected);
}
