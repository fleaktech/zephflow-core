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
package io.fleak.zephflow.sdk;

import static io.fleak.zephflow.lib.commands.SimpleHttpClient.MAX_RESPONSE_SIZE_BYTES;
import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static io.fleak.zephflow.runner.Constants.HTTP_STARTER_WORKFLOW_CONTROLLER_PATH;
import static io.fleak.zephflow.runner.DagExecutor.loadCommands;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.SimpleHttpClient;
import io.fleak.zephflow.lib.commands.filesource.FileSourceDto;
import io.fleak.zephflow.lib.commands.kafkasink.KafkaSinkDto;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceDto;
import io.fleak.zephflow.lib.commands.s3.S3SinkDto;
import io.fleak.zephflow.lib.commands.stdin.StdInSourceDto;
import io.fleak.zephflow.lib.commands.stdout.StdOutDto;
import io.fleak.zephflow.lib.parser.ParserConfigs;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.runner.*;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition.DagNode;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.NonNull;

/**
 * A class that represents a flow of data processing operations. It builds a directed acyclic graph
 * (DAG) of processing nodes.
 */
public class ZephFlow {
  // Static registry to keep track of all ZephFlow instances and their nodes
  private static final Map<String, ZephFlow> flowRegistry = new HashMap<>();

  private static final Map<String, CommandFactory> aggregatedCommands = loadCommands();

  @Getter private final DagNode node;

  private final List<ZephFlow> upstreamFlows;

  private final JobContext jobContext;

  // Private constructor for internal use
  private ZephFlow(DagNode node, List<ZephFlow> upstreamFlows, JobContext jobContext) {
    this.node = node;
    this.upstreamFlows = upstreamFlows;
    this.jobContext = jobContext;

    // Register this instance if it has a node
    if (node != null) {
      flowRegistry.put(node.getId(), this);
    }
  }

  /**
   * Creates a new ZephFlow with no node and no upstream connections. This is usually the starting
   * point for building a flow.
   *
   * @return a new empty ZephFlow
   */
  public static ZephFlow startFlow() {
    return startFlow(
        JobContext.builder()
            .metricTags(
                Map.of(METRIC_TAG_SERVICE, "default_service", METRIC_TAG_ENV, "default_env"))
            .build());
  }

  public static ZephFlow startFlow(JobContext jobContext) {
    validateMetricTags(jobContext.getMetricTags());
    return new ZephFlow(null, new ArrayList<>(), jobContext);
  }

  /**
   * Creates a filter node that will only pass data that satisfies the filter condition.
   *
   * @param condition The filter condition to be evaluated
   * @return A new ZephFlow with the filter node attached
   */
  public ZephFlow filter(String condition) {
    return appendNode(COMMAND_NAME_FILTER, condition);
  }

  /**
   * Creates an assertion node that will only pass data that satisfies the filter condition. those
   * that doesn't pass the filter condition will cause command to throw exceptions
   *
   * @param condition The filter condition to be evaluated
   * @return A new ZephFlow with the filter node attached
   */
  public ZephFlow assertion(String condition) {
    return appendNode(COMMAND_NAME_ASSERTION, condition);
  }

  public ZephFlow parse(ParserConfigs.ParserConfig parserConfig) {
    return appendNode(COMMAND_NAME_PARSER, toJsonString(parserConfig));
  }

  // Method to create a stdin source node
  public ZephFlow stdinSource(EncodingType encodingType) {
    // Create config for stdin source
    StdInSourceDto.Config config =
        StdInSourceDto.Config.builder().encodingType(encodingType).build();

    // Use appendNode to ensure proper connections with upstream nodes
    return appendNode(COMMAND_NAME_STDIN, toJsonString(config));
  }

  public ZephFlow fileSource(String filePath, EncodingType encodingType) {
    FileSourceDto.Config config =
        FileSourceDto.Config.builder().filePath(filePath).encodingType(encodingType).build();
    return appendNode(COMMAND_NAME_FILE_SOURCE, toJsonString(config));
  }

  public ZephFlow eval(String evalExpression) {
    return appendNode(COMMAND_NAME_EVAL, evalExpression);
  }

  public ZephFlow sql(String sql) {
    return appendNode(COMMAND_NAME_SQL_EVAL, sql);
  }

  @SuppressWarnings("unused")
  public ZephFlow s3Sink(String region, String bucket, String folder, EncodingType encodingType) {
    return s3Sink(region, bucket, folder, encodingType, null);
  }

  public ZephFlow s3Sink(
      String region,
      String bucket,
      String folder,
      EncodingType encodingType,
      String s3EndpointOverride) {
    S3SinkDto.Config config =
        S3SinkDto.Config.builder()
            .regionStr(region)
            .bucketName(bucket)
            .keyName(folder)
            .encodingType(encodingType.toString())
            .s3EndpointOverride(s3EndpointOverride)
            .build();
    return appendNode(COMMAND_NAME_S3_SINK, toJsonString(config));
  }

  public ZephFlow kafkaSource(
      @NonNull String broker,
      @NonNull String topic,
      @Nonnull String groupId,
      @NonNull EncodingType encodingType,
      Map<String, String> properties) {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker(broker)
            .topic(topic)
            .groupId(groupId)
            .encodingType(encodingType)
            .properties(properties)
            .build();
    return appendNode(COMMAND_NAME_KAFKA_SOURCE, toJsonString(config));
  }

  public ZephFlow kafkaSink(
      @NonNull String broker,
      @NonNull String topic,
      String partitionKeyFieldExpressionStr,
      @NonNull EncodingType encodingType,
      Map<String, String> properties) {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker(broker)
            .topic(topic)
            .partitionKeyFieldExpressionStr(partitionKeyFieldExpressionStr)
            .encodingType(encodingType.toString())
            .properties(properties)
            .build();
    return appendNode(COMMAND_NAME_KAFKA_SINK, toJsonString(config));
  }

  public ZephFlow stdoutSink(EncodingType encodingType) {
    StdOutDto.Config config = StdOutDto.Config.builder().encodingType(encodingType).build();
    return appendNode(COMMAND_NAME_STDOUT, toJsonString(config));
  }

  public ZephFlow appendNode(String commandName, String configStr) {

    String id = commandName + "_" + generateRandomHash();

    // Create the DAG node with the provided parameters
    DagNode node =
        DagNode.builder()
            .id(id)
            .commandName(commandName)
            .config(configStr)
            .outputs(new ArrayList<>()) // Initialize with empty outputs
            .build();

    // Handle different cases:
    // 1. Regular flow with a node
    // 2. Merged flow with upstream flows

    // Determine upstream flows
    List<ZephFlow> upstreams;

    if (getNode() != null) {
      // This is a regular flow with a node
      upstreams = List.of(this);
    } else {
      // This is a merged flow with upstream flows
      upstreams = new ArrayList<>(upstreamFlows);
    }

    // Create the new flow with the node and upstream connections
    return new ZephFlow(node, upstreams, jobContext);
  }

  // Static method to merge multiple flows into one
  public static ZephFlow merge(ZephFlow... flows) {
    if (flows.length == 0) {
      throw new IllegalArgumentException("At least one flow must be provided for merging");
    }

    // If there's only one flow, return it directly
    if (flows.length == 1) {
      return flows[0];
    }

    // Collect all upstream flows
    List<ZephFlow> upstreamFlows = new ArrayList<>(Arrays.asList(flows));

    // Return a special merged flow that doesn't have its own node
    // The node will be created when this flow is connected to a sink
    return new ZephFlow(
        null, // No intermediate node
        upstreamFlows,
        flows[0].jobContext); // it is assumed that all flows have the same JobContext
  }

  public AdjacencyListDagDefinition buildDag() {
    return buildDag(JobContext.builder().build());
  }

  public AdjacencyListDagDefinition buildDag(JobContext jobContext) {
    List<DagNode> allNodes = new ArrayList<>();
    Map<String, Boolean> visited = new HashMap<>();

    // Collect all nodes in the flow graph (breadth-first traversal)
    collectNodes(this, allNodes, visited);

    // Connect nodes based on upstream relationships
    connectNodes(allNodes);

    // Create and return the DAG definition
    return AdjacencyListDagDefinition.builder().jobContext(jobContext).dag(allNodes).build();
  }

  public void execute(@NonNull String jobId, @NonNull String env, @NonNull String service)
      throws Exception {
    execute(jobId, env, service, new MetricClientProvider.NoopMetricClientProvider());
  }

  public void execute(
      @NonNull String jobId,
      @NonNull String env,
      @NonNull String service,
      MetricClientProvider metricClientProvider)
      throws Exception {
    AdjacencyListDagDefinition adjacencyListDagDefinition = buildDag(jobContext);
    JobConfig jobConfig =
        JobConfig.builder()
            .environment(env)
            .service(service)
            .jobId(jobId)
            .dagDefinition(adjacencyListDagDefinition)
            .build();
    DagExecutor dagExecutor = DagExecutor.createDagExecutor(jobConfig, metricClientProvider);
    dagExecutor.executeDag();
  }

  public DagResult process(List<?> events, NoSourceDagRunner.DagRunConfig runConfig) {
    return process(
        events,
        "default_user",
        runConfig,
        JobContext.builder()
            .metricTags(
                Map.of(METRIC_TAG_SERVICE, "default_service", METRIC_TAG_ENV, "default_env"))
            .build(),
        new MetricClientProvider.NoopMetricClientProvider());
  }

  public DagResult process(
      List<?> events,
      String callingUser,
      NoSourceDagRunner.DagRunConfig runConfig,
      JobContext jobContext,
      MetricClientProvider metricClientProvider) {
    List<RecordFleakData> inputData =
        events.stream().map(o -> ((RecordFleakData) fromObject(o))).toList();

    DagCompiler dagCompiler = new DagCompiler(aggregatedCommands);
    DagRunnerService dagRunnerService = new DagRunnerService(dagCompiler, metricClientProvider);

    AdjacencyListDagDefinition dagDefinition = buildDag(jobContext);
    NoSourceDagRunner noSourceDagRunner =
        dagRunnerService.createForApiBackend(dagDefinition.getDag(), dagDefinition.getJobContext());
    return noSourceDagRunner.run(inputData, callingUser, runConfig);
  }

  public String submitApiEndpoint(String httpStarterHostUrl) throws URISyntaxException {
    AdjacencyListDagDefinition adjacencyListDagDefinition = buildDag();

    httpStarterHostUrl =
        httpStarterHostUrl.endsWith("/")
            ? httpStarterHostUrl.substring(0, httpStarterHostUrl.length() - 1)
            : httpStarterHostUrl;
    URI baseUri = new URI(httpStarterHostUrl);
    URI resolvedUri = baseUri.resolve(HTTP_STARTER_WORKFLOW_CONTROLLER_PATH);

    JsonNode dagJson = convertToJsonNode(adjacencyListDagDefinition.getDag());
    ObjectNode requestJson = OBJECT_MAPPER.createObjectNode();
    requestJson.set("dag", dagJson);
    System.err.println(requestJson);
    SimpleHttpClient simpleHttpClient = SimpleHttpClient.getInstance(MAX_RESPONSE_SIZE_BYTES);
    return simpleHttpClient.callHttpEndpointNoSecureCheck(
        resolvedUri.toString(),
        SimpleHttpClient.HttpMethodType.POST,
        requestJson.toString(),
        List.of("Content-Type: application/json"));
  }

  // Helper method to collect all nodes in the flow graph
  private static void collectNodes(
      ZephFlow flow, List<DagNode> nodes, Map<String, Boolean> visited) {
    if (flow == null || flow.getNode() == null) {
      // If this is a merged flow, collect nodes from all upstream flows
      if (flow != null) {
        for (ZephFlow upstream : flow.upstreamFlows) {
          collectNodes(upstream, nodes, visited);
        }
      }
      return;
    }

    String nodeId = flow.getNode().getId();

    // Skip if already visited
    if (visited.containsKey(nodeId)) {
      return;
    }

    // Mark as visited and add to the list
    visited.put(nodeId, true);
    nodes.add(flow.getNode());

    // Recursively collect upstream nodes
    for (ZephFlow upstream : flow.upstreamFlows) {
      collectNodes(upstream, nodes, visited);
    }
  }

  // Helper method to connect nodes based on upstream relationships
  private static void connectNodes(List<DagNode> nodes) {
    Map<String, DagNode> nodeMap = new HashMap<>();

    // Build node map for quick lookup
    for (DagNode node : nodes) {
      nodeMap.put(node.getId(), node);
    }

    // Iterate through all ZephFlow instances (not DagNode instances)
    for (ZephFlow flow : flowRegistry.values()) {
      // Get the flow's node
      DagNode node = flow.getNode();

      // Skip if this flow doesn't have a node (like intermediate merged flows)
      if (node == null) {
        continue;
      }

      // Skip if the node is not in our collection
      if (!nodeMap.containsKey(node.getId())) {
        continue;
      }

      // Connect upstream nodes to this node
      for (ZephFlow upstream : flow.upstreamFlows) {
        // Skip null upstream nodes (should not happen, but check to be safe)
        if (upstream.getNode() == null) {
          continue;
        }

        String upstreamNodeId = upstream.getNode().getId();
        DagNode upstreamNode = nodeMap.get(upstreamNodeId);

        if (upstreamNode == null) {
          continue; // Skip if upstream node not found
        }

        // Add output from upstream to this node (without conditions)
        upstreamNode.getOutputs().add(node.getId());
      }
    }
  }
}
