/**
 * Copyright 2025 Fleak Tech Inc. // TODO: Update copyright year
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
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A class that represents a flow of data processing operations using a fluent API. It builds a
 * directed acyclic graph (DAG) of processing nodes internally. This class is immutable; methods
 * like filter, sink, etc., return new instances.
 */
@Slf4j
public class ZephFlow {

  // Static map of available command factories, loaded once.
  private static final Map<String, CommandFactory> aggregatedCommands = loadCommands();

  // The DAG node representing the operation defined by this ZephFlow instance.
  // Can be null for intermediate merge flows.
  @Getter private final DagNode node;

  // List of upstream flows that feed data into this flow's node.
  private final List<ZephFlow> upstreamFlows;

  // Job-level context (e.g., metrics tags, properties).
  private final JobContext jobContext;

  // Provider for metric clients.
  private final MetricClientProvider metricClientProvider;

  // Cached runner for the 'process' method (for flows without explicit sources).
  private transient NoSourceDagRunner
      noSourceDagRunner; // Marked transient if serialization is intended

  /**
   * Private constructor for internal use by factory methods and builder methods.
   *
   * @param node The DAG node for this step, or null for merge points.
   * @param upstreamFlows The list of direct upstream flows.
   * @param jobContext The job context.
   * @param metricClientProvider The metric client provider.
   */
  private ZephFlow(
      DagNode node,
      List<ZephFlow> upstreamFlows,
      JobContext jobContext,
      MetricClientProvider metricClientProvider) {
    this.node = node;
    // Ensure upstreamFlows is an immutable list or a defensive copy if needed
    this.upstreamFlows = List.copyOf(upstreamFlows);
    this.jobContext = jobContext;
    this.metricClientProvider = metricClientProvider;
  }

  // --- Static Factory Methods ---

  /**
   * Creates a new ZephFlow builder starting point with default context.
   *
   * @return A new empty ZephFlow instance.
   */
  public static ZephFlow startFlow() {
    return startFlow(
        JobContext.builder()
            .metricTags(
                Map.of(METRIC_TAG_SERVICE, "default_service", METRIC_TAG_ENV, "default_env"))
            .build(),
        new MetricClientProvider.NoopMetricClientProvider());
  }

  /**
   * Creates a new ZephFlow builder starting point with specified JobContext.
   *
   * @param jobContext The job context.
   * @return A new empty ZephFlow instance.
   */
  public static ZephFlow startFlow(JobContext jobContext) {
    return startFlow(jobContext, new MetricClientProvider.NoopMetricClientProvider());
  }

  /**
   * Creates a new ZephFlow builder starting point with specified MetricClientProvider.
   *
   * @param metricClientProvider The metric client provider.
   * @return A new empty ZephFlow instance.
   */
  public static ZephFlow startFlow(MetricClientProvider metricClientProvider) {
    return startFlow(
        JobContext.builder()
            .metricTags(
                Map.of(METRIC_TAG_SERVICE, "default_service", METRIC_TAG_ENV, "default_env"))
            .build(),
        metricClientProvider);
  }

  /**
   * Creates a new ZephFlow builder starting point with specified context and metric provider.
   *
   * @param jobContext The job context.
   * @param metricClientProvider The metric client provider.
   * @return A new empty ZephFlow instance.
   */
  public static ZephFlow startFlow(
      JobContext jobContext, MetricClientProvider metricClientProvider) {
    validateMetricTags(jobContext.getMetricTags());
    // Initial flow has no node and no upstreams
    return new ZephFlow(null, Collections.emptyList(), jobContext, metricClientProvider);
  }

  // --- Builder Methods (Operations) ---

  /**
   * Appends a filter node to the flow.
   *
   * @param condition The filter condition expression.
   * @return A new ZephFlow instance representing the flow with the filter appended.
   */
  public ZephFlow filter(String condition) {
    return appendNode(COMMAND_NAME_FILTER, condition);
  }

  /**
   * Appends an assertion node to the flow. Throws an exception if the condition is not met.
   *
   * @param condition The assertion condition expression.
   * @return A new ZephFlow instance representing the flow with the assertion appended.
   */
  public ZephFlow assertion(String condition) {
    return appendNode(COMMAND_NAME_ASSERTION, condition);
  }

  /**
   * Appends a parser node to the flow.
   *
   * @param parserConfig The configuration for the parser.
   * @return A new ZephFlow instance representing the flow with the parser appended.
   */
  public ZephFlow parse(ParserConfigs.ParserConfig parserConfig) {
    return appendNode(COMMAND_NAME_PARSER, toJsonString(parserConfig));
  }

  /**
   * Appends a standard input source node to the flow.
   *
   * @param encodingType The encoding of the input data.
   * @return A new ZephFlow instance representing the flow with the stdin source appended.
   */
  public ZephFlow stdinSource(EncodingType encodingType) {
    StdInSourceDto.Config config =
        StdInSourceDto.Config.builder().encodingType(encodingType).build();
    return appendNode(COMMAND_NAME_STDIN, toJsonString(config));
  }

  /**
   * Appends a file source node to the flow.
   *
   * @param filePath The path to the input file.
   * @param encodingType The encoding of the file data.
   * @return A new ZephFlow instance representing the flow with the file source appended.
   */
  public ZephFlow fileSource(String filePath, EncodingType encodingType) {
    FileSourceDto.Config config =
        FileSourceDto.Config.builder().filePath(filePath).encodingType(encodingType).build();
    return appendNode(COMMAND_NAME_FILE_SOURCE, toJsonString(config));
  }

  /**
   * Appends an evaluation node to the flow.
   *
   * @param evalExpression The expression to evaluate.
   * @return A new ZephFlow instance representing the flow with the eval node appended.
   */
  public ZephFlow eval(String evalExpression) {
    return appendNode(COMMAND_NAME_EVAL, evalExpression);
  }

  /**
   * Appends an SQL evaluation node to the flow.
   *
   * @param sql The SQL query to execute.
   * @return A new ZephFlow instance representing the flow with the SQL node appended.
   */
  public ZephFlow sql(String sql) {
    return appendNode(COMMAND_NAME_SQL_EVAL, sql);
  }

  /**
   * Appends an S3 sink node to the flow.
   *
   * @param region The AWS region.
   * @param bucket The S3 bucket name.
   * @param folder The target folder/prefix within the bucket.
   * @param encodingType The encoding for the output data.
   * @return A new ZephFlow instance representing the flow with the S3 sink appended.
   */
  @SuppressWarnings("unused") // Part of the public API
  public ZephFlow s3Sink(String region, String bucket, String folder, EncodingType encodingType) {
    return s3Sink(region, bucket, folder, encodingType, null);
  }

  /**
   * Appends an S3 sink node to the flow with an endpoint override.
   *
   * @param region The AWS region.
   * @param bucket The S3 bucket name.
   * @param folder The target folder/prefix within the bucket.
   * @param encodingType The encoding for the output data.
   * @param s3EndpointOverride Optional S3 endpoint override (e.g., for MinIO).
   * @return A new ZephFlow instance representing the flow with the S3 sink appended.
   */
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
            .encodingType(
                encodingType
                    .toString()) // Consider passing EncodingType directly if DTO supports it
            .s3EndpointOverride(s3EndpointOverride)
            .build();
    return appendNode(COMMAND_NAME_S3_SINK, toJsonString(config));
  }

  /**
   * Appends a Kafka source node to the flow.
   *
   * @param broker The Kafka broker list (comma-separated).
   * @param topic The Kafka topic to consume from.
   * @param groupId The Kafka consumer group ID.
   * @param encodingType The encoding of the messages.
   * @param properties Additional Kafka consumer properties.
   * @return A new ZephFlow instance representing the flow with the Kafka source appended.
   */
  public ZephFlow kafkaSource(
      @NonNull String broker,
      @NonNull String topic,
      @Nonnull String groupId, // Consider @NonNull if null is never valid
      @NonNull EncodingType encodingType,
      Map<String, String> properties) {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker(broker)
            .topic(topic)
            .groupId(groupId)
            .encodingType(encodingType)
            .properties(
                properties == null ? Collections.emptyMap() : properties) // Defensive copy/check
            .build();
    return appendNode(COMMAND_NAME_KAFKA_SOURCE, toJsonString(config));
  }

  /**
   * Appends a Kafka sink node to the flow.
   *
   * @param broker The Kafka broker list (comma-separated).
   * @param topic The Kafka topic to produce to.
   * @param partitionKeyFieldExpressionStr Optional expression to determine the partition key field.
   * @param encodingType The encoding for the output messages.
   * @param properties Additional Kafka producer properties.
   * @return A new ZephFlow instance representing the flow with the Kafka sink appended.
   */
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
            .encodingType(
                encodingType
                    .toString()) // Consider passing EncodingType directly if DTO supports it
            .properties(
                properties == null ? Collections.emptyMap() : properties) // Defensive copy/check
            .build();
    return appendNode(COMMAND_NAME_KAFKA_SINK, toJsonString(config));
  }

  /**
   * Appends a standard output sink node to the flow.
   *
   * @param encodingType The encoding for the output data.
   * @return A new ZephFlow instance representing the flow with the stdout sink appended.
   */
  public ZephFlow stdoutSink(EncodingType encodingType) {
    StdOutDto.Config config = StdOutDto.Config.builder().encodingType(encodingType).build();
    return appendNode(COMMAND_NAME_STDOUT, toJsonString(config));
  }

  /**
   * Internal helper method to append a new processing node to the current flow structure.
   *
   * @param commandName The name of the command for the new node.
   * @param configStr The JSON configuration string for the command.
   * @return A new ZephFlow instance representing the flow with the new node appended.
   */
  private ZephFlow appendNode(String commandName, String configStr) {
    // Generate a unique ID for the new node.
    String id = commandName + "_" + generateRandomHash();

    // Create the DAG node definition for this step.
    DagNode newNodeDef =
        DagNode.builder()
            .id(id)
            .commandName(commandName)
            .config(configStr)
            .outputs(new ArrayList<>()) // Outputs will be populated during buildDag traversal
            .build();

    // Determine the upstream flows for the new node.
    // If the current ZephFlow instance has a node, it's the single upstream.
    // If the current ZephFlow instance represents a merge point (no node),
    // then its upstreamFlows become the upstreams for the new node.
    List<ZephFlow> upstreams;
    if (this.node != null) {
      // Current instance is a regular node, becomes the single upstream.
      upstreams = List.of(this);
    } else {
      // Current instance is a merge point, pass its upstreams along.
      upstreams = this.upstreamFlows; // Already unmodifiable
    }

    // Create the new ZephFlow instance representing the appended node.
    // It inherits the jobContext and metricClientProvider.
    return new ZephFlow(newNodeDef, upstreams, this.jobContext, this.metricClientProvider);
  }

  // --- Flow Combination ---

  /**
   * Merges multiple ZephFlow branches into a single flow branch. The resulting ZephFlow instance
   * acts as a placeholder; the actual merge logic happens when a subsequent node is appended to
   * this merged flow.
   *
   * @param flows The ZephFlow instances to merge. Must provide at least one.
   * @return A new ZephFlow instance representing the merge point.
   * @throws IllegalArgumentException if no flows are provided.
   */
  public static ZephFlow merge(ZephFlow... flows) {
    if (flows == null || flows.length == 0) {
      throw new IllegalArgumentException("At least one flow must be provided for merging");
    }

    // If only one flow is provided, simply return it.
    if (flows.length == 1) {
      return flows[0];
    }

    // Collect all provided flows as upstreams for the merge point.
    List<ZephFlow> upstreamFlows = new ArrayList<>(Arrays.asList(flows));

    // --- Merge JobContext ---
    // Note: Simple Map.putAll overwrites duplicate keys.
    Map<String, Serializable> mergedProperties = new HashMap<>();
    Map<String, String> mergedMetricTags = new HashMap<>();
    MetricClientProvider chosenProvider =
        flows[0].metricClientProvider; // Take provider from the first flow

    for (ZephFlow f : flows) {
      if (f.jobContext != null) {
        if (f.jobContext.getOtherProperties() != null) {
          mergedProperties.putAll(f.jobContext.getOtherProperties());
        }
        if (f.jobContext.getMetricTags() != null) {
          mergedMetricTags.putAll(f.jobContext.getMetricTags());
        }
        // Currently, just using the first one encountered.
      }
    }
    JobContext mergedJobContext =
        JobContext.builder().otherProperties(mergedProperties).metricTags(mergedMetricTags).build();
    // --- End Merge JobContext ---

    // Create a special ZephFlow instance with no node, representing the merge point.
    // It holds the merged flows as its upstreams and the merged context.
    return new ZephFlow(
        null, // No node for the merge point itself
        upstreamFlows,
        mergedJobContext,
        chosenProvider);
  }

  // --- DAG Building and Execution ---

  /**
   * Builds the internal DAG representation (AdjacencyListDagDefinition) from the fluent API
   * structure. Traverses the graph defined by ZephFlow instances and their upstreamFlows.
   *
   * @return The AdjacencyListDagDefinition representing the constructed flow.
   */
  public AdjacencyListDagDefinition buildDag() {
    // Map to store constructed DagNode instances, keyed by their ID.
    // This prevents duplicate node creation and allows easy lookup.
    Map<String, DagNode> nodeMap = new HashMap<>();

    // Set to keep track of visited ZephFlow instances during traversal
    // to handle cycles (though DAGs shouldn't have them) and merge points efficiently.
    // Use IdentityHashMap or rely on unique node IDs if ZephFlow instances might be recreated.
    // Using a simple Set assumes object identity or correct hashCode/equals if applicable.
    Set<ZephFlow> visited = Collections.newSetFromMap(new IdentityHashMap<>());

    // Start the recursive traversal from the current ZephFlow instance (typically the sink).
    buildAndConnectRecursive(this, nodeMap, visited);

    // The nodeMap now contains all the DagNode instances with their outputs correctly populated.
    // Create the final DAG definition.
    return AdjacencyListDagDefinition.builder()
        .jobContext(this.jobContext) // Use the context of the final flow instance
        .dag(new ArrayList<>(nodeMap.values())) // Collect all unique nodes
        .build();
  }

  /**
   * Recursive helper method to build the DAG. Traverses the ZephFlow graph, creates DagNode
   * instances, and connects them. Returns the list of "leaf" DagNodes resulting from the processed
   * subgraph.
   *
   * @param currentFlow The current ZephFlow instance being processed.
   * @param nodeMap A map accumulating the created DagNode instances (ID -> DagNode).
   * @param visited A set tracking visited ZephFlow instances to prevent infinite loops.
   * @return A list of DagNodes that are the effective outputs of the subgraph rooted at
   *     currentFlow. Returns an empty list if currentFlow is null. Returns a list containing the
   *     single node if currentFlow represents a node. Returns a list containing all leaf nodes from
   *     upstreams if currentFlow is a merge point.
   */
  private List<DagNode> buildAndConnectRecursive(
      ZephFlow currentFlow, Map<String, DagNode> nodeMap, Set<ZephFlow> visited) {

    // Base case 1: Null flow
    if (currentFlow == null) {
      return Collections.emptyList();
    }

    // Base case 2: Already visited this ZephFlow instance.
    if (!visited.add(currentFlow)) {
      // If visited, return the node if it exists, otherwise empty list (e.g., revisited merge
      // point)
      if (currentFlow.getNode() != null) {
        // Ensure the node is in the map (it should be if visited)
        DagNode existingNode = nodeMap.get(currentFlow.getNode().getId());
        return existingNode != null ? List.of(existingNode) : Collections.emptyList();
      } else {
        // Revisiting a merge point - its leaves were already returned up the stack.
        return Collections.emptyList();
      }
    }

    // --- Process Current Node (if it exists) ---
    DagNode currentNode = null;
    if (currentFlow.getNode() != null) {
      String nodeId = currentFlow.getNode().getId();
      // Get existing node or create a new one (with a mutable outputs list).
      currentNode =
          nodeMap.computeIfAbsent(
              nodeId,
              id ->
                  DagNode.builder()
                      .id(currentFlow.getNode().getId())
                      .commandName(currentFlow.getNode().getCommandName())
                      .config(currentFlow.getNode().getConfig())
                      .outputs(new ArrayList<>()) // Initialize mutable list
                      .build());
    }
    // If currentFlow.getNode() is null, this is a merge point, currentNode remains null.

    // --- Recurse for Upstreams and Collect Leaf Nodes ---
    List<DagNode> allUpstreamLeafNodes = new ArrayList<>();
    for (ZephFlow upstreamFlow : currentFlow.upstreamFlows) {
      // Recursively build the upstream part of the graph & get its leaf nodes.
      List<DagNode> upstreamLeaves = buildAndConnectRecursive(upstreamFlow, nodeMap, visited);
      allUpstreamLeafNodes.addAll(upstreamLeaves);
    }

    // --- Connect Upstream Leaves to Current Node ---
    if (currentNode != null) { // Only connect if the current flow represents an actual node
      for (DagNode upstreamLeaf : allUpstreamLeafNodes) {
        if (upstreamLeaf == null) {
          // Should not be null based on recursion logic, but check defensively
          continue;
        }
        // Add the current node's ID to the upstream leaf node's outputs.
        List<String> outputs = upstreamLeaf.getOutputs();
        // Ensure no duplicate connections are added.
        if (outputs != null && !outputs.contains(currentNode.getId())) {
          outputs.add(currentNode.getId());
        }
      }
    }

    // --- Determine Return Value ---
    if (currentNode != null) {
      // If this flow has a node, it's the single leaf for this path.
      return List.of(currentNode);
    } else {
      // If this flow is a merge point (no node), pass up the leaves from its upstreams.
      // Remove nulls just in case, although they shouldn't occur with current logic.
      return allUpstreamLeafNodes.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }
  }

  /**
   * Executes the defined ZephFlow DAG locally.
   *
   * @param jobId A unique identifier for this job execution.
   * @param env The environment identifier (e.g., "dev", "prod").
   * @param service The service identifier.
   * @throws Exception if DAG execution fails.
   */
  public void execute(@NonNull String jobId, @NonNull String env, @NonNull String service)
      throws Exception { // Consider more specific exceptions
    AdjacencyListDagDefinition adjacencyListDagDefinition = buildDag();
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

  /**
   * Processes a list of input events using the defined DAG structure, assuming no source nodes are
   * defined in the flow itself (e.g., for testing or API backends). Uses a default calling user
   * "default_user".
   *
   * @param events The list of input events (expected to be convertible to RecordFleakData).
   * @param runConfig Configuration for the DAG run.
   * @return The result of the DAG execution.
   */
  public DagResult process(List<?> events, NoSourceDagRunner.DagRunConfig runConfig) {
    return process(events, "default_user", runConfig);
  }

  /**
   * Processes a list of input events using the defined DAG structure, assuming no source nodes are
   * defined in the flow itself.
   *
   * @param events The list of input events (expected to be convertible to RecordFleakData).
   * @param callingUser Identifier for the user initiating the processing.
   * @param runConfig Configuration for the DAG run.
   * @return The result of the DAG execution.
   * @throws ClassCastException if events cannot be converted to RecordFleakData. // Document this
   */
  public DagResult process(
      List<?> events, String callingUser, NoSourceDagRunner.DagRunConfig runConfig) {
    List<RecordFleakData> inputData =
        events.stream().map(o -> ((RecordFleakData) fromObject(o))).toList();

    // Initialize the runner if it hasn't been already for this ZephFlow instance.
    // Note: Caching is per ZephFlow instance. Since instances are immutable,
    // this cache is only reused if process() is called multiple times on the *exact same*
    // final ZephFlow object returned by a sink/builder method.
    if (noSourceDagRunner == null) {
      DagCompiler dagCompiler = new DagCompiler(aggregatedCommands);
      DagRunnerService dagRunnerService = new DagRunnerService(dagCompiler, metricClientProvider);

      AdjacencyListDagDefinition dagDefinition = buildDag();
      noSourceDagRunner =
          dagRunnerService.createForApiBackend(
              dagDefinition.getDag(), dagDefinition.getJobContext());
    }
    return noSourceDagRunner.run(inputData, callingUser, runConfig);
  }

  /**
   * Submits the defined DAG to a remote ZephFlow HTTP starter endpoint.
   *
   * @param httpStarterHostUrl The base URL of the HTTP starter service.
   * @return The response body from the submission endpoint.
   * @throws URISyntaxException if the provided URL is invalid.
   * @throws RuntimeException wrapping HTTP client errors. // Document potential exceptions
   */
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

    log.info("Submitting DAG: {}", requestJson);

    SimpleHttpClient simpleHttpClient = SimpleHttpClient.getInstance(MAX_RESPONSE_SIZE_BYTES);
    return simpleHttpClient.callHttpEndpointNoSecureCheck(
        resolvedUri.toString(),
        SimpleHttpClient.HttpMethodType.POST,
        requestJson.toString(),
        List.of("Content-Type: application/json"));
  }
}
