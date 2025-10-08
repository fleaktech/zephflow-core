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
package io.fleak.zephflow.lib.dag;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Created by bolei on 8/12/24 */
class DagTest {

  private Dag<String> testDag;

  @BeforeEach
  void setUp() {
    List<Node<String>> testNodes =
        Arrays.asList(
            Node.<String>builder().id("A").nodeContent("Node A").build(),
            Node.<String>builder().id("B").nodeContent("Node B").build(),
            Node.<String>builder().id("C").nodeContent("Node C").build(),
            Node.<String>builder().id("D").nodeContent("Node D").build());

    List<Edge> testEdges =
        Arrays.asList(
            new Edge("A", "B"), new Edge("A", "C"), new Edge("B", "D"), new Edge("C", "D"));

    testDag = new Dag<>(testNodes, testEdges);
  }

  @Test
  void testLookupNode() {
    Node<String> node = Node.<String>builder().id("1").build();

    Dag<String> dag = new Dag<>(List.of(node), Collections.emptyList());

    assertEquals(node, dag.lookupNode("1"));
  }

  @Test
  void testUpstreamEdges() {
    Edge edge = new Edge("1", "2");
    Dag<String> dag = new Dag<>(List.of(), List.of(edge));
    assertEquals(List.of(edge), dag.upstreamEdges("2"));
  }

  @Test
  public void testValidConnectedAcyclicGraph() throws Exception {
    List<Node<String>> nodes =
        List.of(
            Node.<String>builder().id("A").build(),
            Node.<String>builder().id("B").build(),
            Node.<String>builder().id("C").build(),
            Node.<String>builder().id("D").build(),
            Node.<String>builder().id("E").build(),
            Node.<String>builder().id("F").build());
    List<Edge> edges =
        List.of(
            new Edge("A", "B"),
            new Edge("B", "C"),
            new Edge("C", "D"),
            new Edge("D", "E"),
            new Edge("E", "F"));
    Dag<String> dag = new Dag<>(nodes, edges);

    dag.validate(true, n -> false, n -> false);
  }

  @Test
  public void testDisconnectedGraph() {
    List<Node<String>> nodes =
        List.of(
            Node.<String>builder().id("A").build(),
            Node.<String>builder().id("B").build(),
            Node.<String>builder().id("C").build(),
            Node.<String>builder().id("D").build(),
            Node.<String>builder().id("E").build(),
            Node.<String>builder().id("F").build());
    List<Edge> edges =
        List.of(new Edge("A", "B"), new Edge("B", "C"), new Edge("D", "E"), new Edge("E", "F"));
    Dag<String> dag = new Dag<>(nodes, edges);

    try {
      dag.validate(true, n -> false, n -> false); // Exception should be thrown
      fail();
    } catch (Exception e) {
      assertEquals("Graph is not connected", e.getMessage());
    }
  }

  @Test
  public void testGraphWithCycle() {
    List<Node<String>> nodes =
        List.of(
            Node.<String>builder().id("A").build(),
            Node.<String>builder().id("B").build(),
            Node.<String>builder().id("C").build(),
            Node.<String>builder().id("D").build(),
            Node.<String>builder().id("E").build(),
            Node.<String>builder().id("F").build());
    List<Edge> edges =
        List.of(
            new Edge("A", "B"),
            new Edge("B", "C"),
            new Edge("C", "A"),
            new Edge("D", "E"),
            new Edge("E", "F"),
            new Edge("F", "D"),
            new Edge("C", "D"));
    Dag<String> dag = new Dag<>(nodes, edges);

    try {
      dag.validate(true, n -> false, n -> false); // Exception should be thrown
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Graph has a cycle"));
    }
  }

  @Test
  public void testGraphWithCycle2() {
    List<Node<String>> nodes = List.of(Node.<String>builder().id("A").build());
    List<Edge> edges = List.of(new Edge("A", "A"));
    Dag<String> dag = new Dag<>(nodes, edges);

    try {
      dag.validate(true, n -> false, n -> false); // Exception should be thrown
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Graph has a cycle"));
    }
  }

  @Test
  public void testGraphWithCycle3() {
    List<Node<String>> nodes =
        List.of(Node.<String>builder().id("A").build(), Node.<String>builder().id("B").build());
    List<Edge> edges = List.of(new Edge("A", "B"), new Edge("B", "A"));
    Dag<String> dag = new Dag<>(nodes, edges);

    try {
      dag.validate(true, n -> false, n -> false); // Exception should be thrown
      fail();
    } catch (Exception e) {
      assertEquals("Graph has a cycle: A -> B", e.getMessage());
    }
  }

  @Test
  public void testInvalidEdgeFromOrTo() {
    List<Node<String>> nodes =
        List.of(Node.<String>builder().id("A").build(), Node.<String>builder().id("B").build());
    List<Edge> edges = List.of(new Edge("A", "C"));
    Dag<String> dag = new Dag<>(nodes, edges);

    try {
      dag.validate(true, n -> false, n -> false); // Exception should be thrown
      fail();
    } catch (Exception e) {
      assertEquals(
          "Edge.from or Edge.to value is not a valid DagNode.nodeId for edge: Edge(from=A, to=C)",
          e.getMessage());
    }
  }

  @Test
  public void testGetEntryNodes() {
    // In our test DAG, only A is an entry node (has no incoming edges)
    List<Node<String>> entryNodes = testDag.getEntryNodes();

    assertEquals(1, entryNodes.size());
    assertEquals("A", entryNodes.get(0).getId());
  }

  @Test
  public void testDownstreamEdges() {
    // Node A has two downstream edges: A->B and A->C
    List<Edge> downstreamEdgesA = testDag.downstreamEdges("A");
    assertEquals(2, downstreamEdgesA.size());

    // Verify that edges have correct targets
    boolean hasEdgeToB = false;
    boolean hasEdgeToC = false;

    for (Edge edge : downstreamEdgesA) {
      if (edge.getTo().equals("B")) hasEdgeToB = true;
      if (edge.getTo().equals("C")) hasEdgeToC = true;
    }

    assertTrue(hasEdgeToB);
    assertTrue(hasEdgeToC);

    // Node D has no downstream edges
    List<Edge> downstreamEdgesD = testDag.downstreamEdges("D");
    assertTrue(downstreamEdgesD.isEmpty());
  }

  @Test
  public void testMultipleUpstreamEdges() {
    // Node D has two upstream edges: B->D and C->D
    List<Edge> upstreamEdgesD = testDag.upstreamEdges("D");
    assertEquals(2, upstreamEdgesD.size());

    // Verify that edges have correct sources
    boolean hasEdgeFromB = false;
    boolean hasEdgeFromC = false;

    for (Edge edge : upstreamEdgesD) {
      if (edge.getFrom().equals("B")) hasEdgeFromB = true;
      if (edge.getFrom().equals("C")) hasEdgeFromC = true;
    }

    assertTrue(hasEdgeFromB);
    assertTrue(hasEdgeFromC);
  }

  @Test
  public void testLookupNodeWithNodeContent() {
    Node<String> nodeA = testDag.lookupNode("A");
    assertEquals("Node A", nodeA.getNodeContent());
  }

  @Test
  public void testLookupNonExistentNode() {
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> testDag.lookupNode("Z"));

    assertEquals("Node with ID Z not found", exception.getMessage());
  }

  @Test
  public void testNoUpstreamEdges() {
    // Node A has no upstream edges
    List<Edge> upstreamEdgesA = testDag.upstreamEdges("A");
    assertTrue(upstreamEdgesA.isEmpty());
  }

  @Test
  public void testFromAdjacencyList() {
    // Create an adjacency list representation
    List<AdjacencyListDagDefinition.DagNode> dagNodes = new ArrayList<>();

    AdjacencyListDagDefinition.DagNode nodeA = new AdjacencyListDagDefinition.DagNode();
    nodeA.setId("A");
    nodeA.setCommandName("Command A");
    nodeA.setConfig(Map.of("k", "Config A"));

    List<String> outputsA = new ArrayList<>();
    String outputAB = "B";
    outputsA.add(outputAB);

    nodeA.setOutputs(outputsA);

    AdjacencyListDagDefinition.DagNode nodeB = new AdjacencyListDagDefinition.DagNode();
    nodeB.setId("B");
    nodeB.setCommandName("Command B");
    nodeB.setConfig(Map.of("k", "Config B"));
    nodeB.setOutputs(new ArrayList<>()); // No outputs

    dagNodes.add(nodeA);
    dagNodes.add(nodeB);

    Dag<RawDagNode> dag = Dag.fromAdjacencyList(dagNodes);

    // Verify nodes
    assertEquals(2, dag.getNodes().size());

    // Verify edges
    assertEquals(1, dag.getEdges().size());

    // Verify node content
    Node<RawDagNode> convertedNodeA = dag.lookupNode("A");
    assertEquals("Command A", convertedNodeA.getNodeContent().getCommandName());
    assertEquals(Map.of("k", "Config A"), convertedNodeA.getNodeContent().getArg());

    // Verify edge
    List<Edge> downstreamEdgesA = dag.downstreamEdges("A");
    assertEquals(1, downstreamEdgesA.size());
    Edge edgeAB = downstreamEdgesA.get(0);
    assertEquals("B", edgeAB.getTo());
  }

  @Test
  public void testFromAdjacencyList_Regression() {
    AdjacencyListDagDefinition.DagNode nodeA = new AdjacencyListDagDefinition.DagNode();
    nodeA.setId("A");
    nodeA.setCommandName("Command A");
    nodeA.setConfig(Map.of("k", "Config A"));

    AdjacencyListDagDefinition.DagNode nodeB = new AdjacencyListDagDefinition.DagNode();
    nodeB.setId("B");
    nodeB.setCommandName("Command B");
    nodeB.setConfig(Map.of("k", "Config B"));

    Dag<RawDagNode> dag = Dag.fromAdjacencyList(List.of(nodeA, nodeB));
    assertTrue(dag.getEdges().isEmpty());
  }

  @Test
  public void testEmptyNodesValidation() {
    Dag<String> dag = new Dag<>(List.of(), List.of());

    try {
      dag.validate(true, n -> false, n -> false);
      fail("Expected exception was not thrown");
    } catch (Exception e) {
      assertEquals("Graph nodes must not be empty", e.getMessage());
    }
  }

  @Test
  public void testToString() {
    String dagString = testDag.toString();
    assertNotNull(dagString);
    assertTrue(dagString.contains("\"nodes\""));
    assertTrue(dagString.contains("\"edges\""));
  }
}
