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
package io.fleak.zephflow.runner.dag;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.*;
import org.apache.commons.lang3.tuple.Pair;

@Getter
@NoArgsConstructor
public class Dag<T> {

  private final List<Node<T>> nodes = new ArrayList<>();
  private final List<Edge> edges = new ArrayList<>();

  // Index maps for faster lookups
  @Getter(AccessLevel.NONE)
  private transient Map<String, Node<T>> nodeIndex = new HashMap<>();

  @Getter(AccessLevel.NONE)
  private transient Map<String, List<Edge>> incomingEdgesIndex = new HashMap<>();

  @Getter(AccessLevel.NONE)
  private transient Map<String, List<Edge>> outgoingEdgesIndex = new HashMap<>();

  /**
   * Custom constructor to create a DAG with nodes and edges. Builds indexes immediately upon
   * creation.
   *
   * @param nodes the List of nodes
   * @param edges the List of edges
   */
  public Dag(List<Node<T>> nodes, List<Edge> edges) {
    if (nodes != null) {
      this.nodes.addAll(nodes);
    }
    if (edges != null) {
      this.edges.addAll(edges);
    }
    buildIndexes();
  }

  /**
   * Initializes the indexes for nodes and edges. This should be called whenever nodes or edges are
   * modified.
   */
  private void buildIndexes() {
    // Build node index
    nodeIndex = new HashMap<>();
    for (Node<T> node : nodes) {
      nodeIndex.put(node.getId(), node);
    }

    // Build edge indexes
    incomingEdgesIndex = new HashMap<>();
    outgoingEdgesIndex = new HashMap<>();

    for (Edge edge : edges) {
      // Index incoming edges
      incomingEdgesIndex.computeIfAbsent(edge.getTo(), k -> new ArrayList<>()).add(edge);

      // Index outgoing edges
      outgoingEdgesIndex.computeIfAbsent(edge.getFrom(), k -> new ArrayList<>()).add(edge);
    }
  }

  public Node<T> lookupNode(String nodeId) {
    Node<T> node = nodeIndex.get(nodeId);
    if (node == null) {
      throw new IllegalArgumentException("Node with ID " + nodeId + " not found");
    }
    return node;
  }

  public List<Edge> upstreamEdges(String nodeId) {
    return incomingEdgesIndex.getOrDefault(nodeId, Collections.emptyList());
  }

  public List<Edge> downstreamEdges(String nodeId) {
    // Ensure indexes are built
    if (outgoingEdgesIndex.isEmpty() && !edges.isEmpty()) {
      buildIndexes();
    }

    return outgoingEdgesIndex.getOrDefault(nodeId, Collections.emptyList());
  }

  public List<Node<T>> getEntryNodes() {
    // Ensure indexes are built
    if (incomingEdgesIndex.isEmpty() && !edges.isEmpty()) {
      buildIndexes();
    }

    // Find nodes that have no incoming edges (source nodes)
    return nodes.stream()
        .filter(
            node ->
                !incomingEdgesIndex.containsKey(node.getId())
                    || incomingEdgesIndex.get(node.getId()).isEmpty())
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return toJsonString(this);
  }

  private boolean isConnected() {
    if (nodes.isEmpty()) {
      return false;
    }

    Set<String> allNodeIds = nodeIndex.keySet();
    Set<String> visited = new HashSet<>();

    // Start from the first node
    String startNodeId = nodes.stream().findFirst().orElseThrow().getId();
    traverseIgnoringDirection(startNodeId, visited);

    // If visited nodes are equal to all nodes, then the graph is weakly connected
    return visited.equals(allNodeIds);
  }

  /**
   *
   *
   * <pre>
   * validate the dag is:
   *    1. connected
   *    2. directed acyclic graph
   *    3. contains only one start node (no input edge)
   *    4. sink nodes have no outgoing edges
   * </pre>
   *
   * @param checkConnected Whether to check if the graph is connected
   * @param isSinkNode A predicate that determines if a node is a sink node
   * @throws Exception If validation fails
   */
  public void validate(
      boolean checkConnected,
      @NonNull Predicate<Node<T>> isSourceNode,
      @NonNull Predicate<Node<T>> isSinkNode)
      throws Exception {
    if (nodes.isEmpty()) {
      throw new Exception("Graph nodes must not be empty");
    }

    // Ensure indexes are built
    buildIndexes();

    // validate Edge.from and Edge.to values
    for (Edge edge : edges) {
      if (!nodeIndex.containsKey(edge.getFrom()) || !nodeIndex.containsKey(edge.getTo())) {
        throw new Exception(
            "Edge.from or Edge.to value is not a valid DagNode.nodeId for edge: " + edge);
      }
    }

    // Check if the graph is connected
    if (checkConnected && !isConnected()) {
      throw new Exception("Graph is not connected");
    }

    // Check if the graph has cycles
    List<String> visited = new ArrayList<>();
    List<String> cycleNodes;
    for (String nodeId : nodeIndex.keySet()) {
      if ((cycleNodes = hasCycle(nodeId, visited, new ArrayList<>())) != null) {
        String cycleString = String.join(" -> ", cycleNodes);
        throw new Exception("Graph has a cycle: " + cycleString);
      }
    }

    // Check that sink nodes have no outgoing edges
    for (Node<T> node : nodes) {
      if (!isSinkNode.test(node)) {
        continue;
      }
      List<Edge> outgoingEdges = downstreamEdges(node.getId());
      if (outgoingEdges.isEmpty()) {
        continue;
      }
      String downstream = outgoingEdges.get(0).getTo();
      throw new IllegalStateException(
          "Invalid DAG: Sink node '"
              + node.getId()
              + "' has outgoing connections to node '"
              + downstream
              + "'. Sink nodes must be terminal nodes.");
    }

    // Check that source nodes have no incoming edges
    for (Node<T> node : nodes) {
      if (!isSourceNode.test(node)) {
        continue;
      }
      List<Edge> incomingEdges = upstreamEdges(node.getId());
      if (incomingEdges.isEmpty()) {
        continue;
      }
      String upstream = incomingEdges.get(0).getFrom();
      throw new IllegalStateException(
          "Invalid DAG: Source node '"
              + node.getId()
              + "' has incoming connections from node '"
              + upstream
              + "'. Source nodes cannot have incoming connections.");
    }
  }

  private void traverseIgnoringDirection(String nodeId, Set<String> visited) {
    if (visited.contains(nodeId)) return;

    visited.add(nodeId);

    // Look for neighbors ignoring the direction of the edges
    // Use the edge indexes for faster lookups
    List<Edge> outgoing = outgoingEdgesIndex.getOrDefault(nodeId, Collections.emptyList());
    for (Edge edge : outgoing) {
      traverseIgnoringDirection(edge.getTo(), visited);
    }

    List<Edge> incoming = incomingEdgesIndex.getOrDefault(nodeId, Collections.emptyList());
    for (Edge edge : incoming) {
      traverseIgnoringDirection(edge.getFrom(), visited);
    }
  }

  private List<String> hasCycle(String nodeId, List<String> visited, List<String> path) {
    if (path.contains(nodeId)) {
      return path.subList(path.indexOf(nodeId), path.size()); // Return the cycle path
    }
    if (visited.contains(nodeId)) return null;

    visited.add(nodeId);
    path.add(nodeId);

    // Use the outgoing edges index for faster lookup
    List<Edge> outgoingEdges = outgoingEdgesIndex.getOrDefault(nodeId, Collections.emptyList());
    for (Edge edge : outgoingEdges) {
      List<String> cyclePath = hasCycle(edge.getTo(), visited, path);
      if (cyclePath != null) return cyclePath;
    }

    path.remove(path.size() - 1); // Remove the last element, which is nodeId
    return null;
  }

  /**
   * Converts a DAG from adjacency list representation to a nodes and edges representation.
   *
   * @param dagNodes List of DagNode objects in adjacency list representation
   * @return A Dag object with nodes and edges
   */
  public static Dag<RawDagNode> fromAdjacencyList(
      List<AdjacencyListDagDefinition.DagNode> dagNodes) {
    // Prepare collections for nodes and edges
    List<Node<RawDagNode>> nodes = new ArrayList<>();
    List<Edge> edges = new ArrayList<>();

    // Create nodes
    for (AdjacencyListDagDefinition.DagNode dagNode : dagNodes) {
      RawDagNode rawDagNode =
          RawDagNode.builder()
              .commandName(dagNode.getCommandName())
              .arg(dagNode.getConfig())
              .build();

      Node<RawDagNode> node =
          Node.<RawDagNode>builder().id(dagNode.getId()).nodeContent(rawDagNode).build();

      nodes.add(node);
    }

    // Check if we need to create a linear DAG (when none of the nodes have defined outputs)
    boolean isLinearDag = true;
    for (AdjacencyListDagDefinition.DagNode dagNode : dagNodes) {
      if (dagNode.getOutputs() != null && !dagNode.getOutputs().isEmpty()) {
        isLinearDag = false;
        break;
      }
    }

    if (isLinearDag) {
      // Create edges for a linear DAG based on the order of nodes in the list
      for (int i = 0; i < dagNodes.size() - 1; i++) {
        String fromNodeId = dagNodes.get(i).getId();
        String toNodeId = dagNodes.get(i + 1).getId();

        // Create edge
        Edge edge = Edge.builder().from(fromNodeId).to(toNodeId).build();
        edges.add(edge);
      }
    } else {
      // Create edges as defined in the outputs
      for (AdjacencyListDagDefinition.DagNode dagNode : dagNodes) {
        String fromNodeId = dagNode.getId();

        for (String targetId : dagNode.getOutputs()) {
          // Create a direct edge from the source to the target
          Edge edge = Edge.builder().from(fromNodeId).to(targetId).build();

          edges.add(edge);
        }
      }
    }

    // Create the DAG with all nodes and edges at once
    return new Dag<>(nodes, edges);
  }

  /**
   * Splits a DAG into two parts: one containing only entry nodes and edges from the entry nodes.
   * Another containing the rest as a subDAG. An entry node is a node that has no incoming edges.
   *
   * @param dag The original DAG to split
   * @return A pair containing (entryNodesDag, restOfDag)
   */
  public static <U> Pair<Dag<U>, Dag<U>> splitEntryNodesAndRest(Dag<U> dag) {
    // Get entry nodes (nodes with no incoming edges)
    List<Node<U>> entryNodes = dag.getEntryNodes();

    // Get entry node IDs for quick lookup
    List<String> entryNodeIds = entryNodes.stream().map(Node::getId).toList();

    // Prepare collections for the two new DAGs
    List<Node<U>> entryNodesList = new ArrayList<>(entryNodes);
    List<Node<U>> restNodesList = new ArrayList<>();
    List<Edge> entryEdgesList = new ArrayList<>();
    List<Edge> restEdgesList = new ArrayList<>();

    // Add non-entry nodes to restOfDag
    for (Node<U> node : dag.getNodes()) {
      if (!entryNodeIds.contains(node.getId())) {
        restNodesList.add(node);
      }
    }

    // Process edges
    for (Edge edge : dag.getEdges()) {
      String fromId = edge.getFrom();
      if (entryNodeIds.contains(fromId)) {
        // If edge starts from an entry node, add to entryNodesDag
        entryEdgesList.add(edge);
      } else {
        // If edge starts from a non-entry node, add to restOfDag
        restEdgesList.add(edge);
      }
    }

    // Create the DAGs with all nodes and edges at once
    Dag<U> entryNodesDag = new Dag<>(entryNodesList, entryEdgesList);
    Dag<U> restOfDag = new Dag<>(restNodesList, restEdgesList);

    return Pair.of(entryNodesDag, restOfDag);
  }
}
