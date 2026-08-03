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
package io.fleak.zephflow.lib.commands.azureeventhub;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;
import static org.junit.jupiter.api.Assertions.*;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.azureeventhubsink.AzureEventHubSinkCommand;
import io.fleak.zephflow.lib.commands.azureeventhubsink.AzureEventHubSinkCommandFactory;
import io.fleak.zephflow.lib.commands.azureeventhubsink.AzureEventHubSinkDto;
import io.fleak.zephflow.lib.commands.azureeventhubsource.AzureEventHubSourceCommand;
import io.fleak.zephflow.lib.commands.azureeventhubsource.AzureEventHubSourceCommandFactory;
import io.fleak.zephflow.lib.commands.azureeventhubsource.AzureEventHubSourceDto;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.azure.EventHubsEmulatorContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end test against the real Azure Event Hubs emulator (+ Azurite for checkpoint storage):
 * the sink publishes events, the source consumes them back and persists partition checkpoints to
 * Azurite. Requires Docker.
 */
@Slf4j
@Tag("integration")
@Testcontainers
public class AzureEventHubIntegrationTest {

  private static final String EVENT_HUB_NAME = "eh1";
  // A second, multi-partition hub: partition-key co-location is unobservable with one partition.
  private static final String KEYED_EVENT_HUB_NAME = "eh2";
  private static final String CONSUMER_GROUP = "cg1";
  private static final String CHECKPOINT_CONTAINER = "checkpoints";

  private static final String EMULATOR_CONFIG_JSON =
      """
      {
        "UserConfig": {
          "NamespaceConfig": [
            {
              "Type": "EventHub",
              "Name": "emulatorNs1",
              "Entities": [
                {
                  "Name": "eh1",
                  "PartitionCount": "1",
                  "ConsumerGroups": [ { "Name": "cg1" } ]
                },
                {
                  "Name": "eh2",
                  "PartitionCount": "4",
                  "ConsumerGroups": [ { "Name": "cg1" } ]
                }
              ]
            }
          ],
          "LoggingConfig": { "Type": "File" }
        }
      }
      """;

  private static final Network NETWORK = Network.newNetwork();

  private static AzuriteContainer azurite;
  private static EventHubsEmulatorContainer eventHubs;

  @BeforeAll
  static void startContainers() {
    azurite =
        new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.35.0").withNetwork(NETWORK);
    azurite.start();

    eventHubs =
        new EventHubsEmulatorContainer("mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.1.0")
            .acceptLicense()
            .withNetwork(NETWORK)
            .withAzuriteContainer(azurite)
            .withConfig(Transferable.of(EMULATOR_CONFIG_JSON));
    eventHubs.start();

    // The blob-backed checkpoint store requires its container to already exist.
    BlobContainerClient container =
        new BlobContainerClientBuilder()
            .connectionString(azurite.getConnectionString())
            .containerName(CHECKPOINT_CONTAINER)
            .buildClient();
    container.createIfNotExists();
  }

  @AfterAll
  static void stopContainers() {
    if (eventHubs != null) {
      eventHubs.stop();
    }
    if (azurite != null) {
      azurite.stop();
    }
    NETWORK.close();
  }

  @Test
  void publishesAndConsumesEventsWithCheckpointing() throws Exception {
    int eventCount = 5;

    publishEvents(eventCount);

    CollectingAcceptor acceptor = new CollectingAcceptor(eventCount);
    AzureEventHubSourceCommand sourceCommand = buildSourceCommand();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(
          () -> {
            sourceCommand.initialize(new MetricClientProvider.NoopMetricClientProvider());
            sourceCommand.execute("test_user", acceptor);
          });

      boolean received = acceptor.latch.await(90, TimeUnit.SECONDS);
      assertTrue(received, "did not receive all events within timeout");

      List<Integer> ids =
          acceptor.received.stream()
              .map(r -> ((Number) r.unwrap().get("id")).intValue())
              .sorted()
              .collect(Collectors.toList());
      assertEquals(List.of(0, 1, 2, 3, 4), ids);

      // Checkpoint path exercised end-to-end: a checkpoint blob was written to Azurite.
      assertTrue(checkpointBlobWritten(), "expected a checkpoint blob to be written to Azurite");
    } finally {
      sourceCommand.terminate();
      executor.shutdownNow();
      //noinspection ResultOfMethodCallIgnored
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  /**
   * Regression test for FLE-2242. A numeric partition key resolved to null, so events were sent
   * unkeyed and one id's events were spread over every partition. Mirrors the manual reproduction:
   * ten events with ids 1x1, 2x2, 3x3, 4x4 into a four-partition hub, read back per partition.
   */
  @Test
  void numericPartitionKeyKeepsEachKeyInOnePartition() throws Exception {
    AzureEventHubSinkDto.Config config =
        AzureEventHubSinkDto.Config.builder()
            .connectionString(eventHubs.getConnectionString())
            .eventHubName(KEYED_EVENT_HUB_NAME)
            .encodingType("JSON_OBJECT")
            .partitionKeyFieldExpressionStr("$.id")
            .build();

    AzureEventHubSinkCommand sink =
        (AzureEventHubSinkCommand)
            new AzureEventHubSinkCommandFactory().createCommand("sink", TestUtils.JOB_CONTEXT);
    sink.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());

    List<RecordFleakData> events = new ArrayList<>();
    for (int id = 1; id <= 4; id++) {
      for (int copy = 0; copy < id; copy++) {
        events.add((RecordFleakData) FleakData.wrap(Map.of("id", id, "copy", copy)));
      }
    }

    try {
      ScalarSinkCommand.SinkResult result =
          sink.writeToSink(events, "test_user", sink.getExecutionContext());
      assertEquals(events.size(), result.getSuccessCount());
    } finally {
      sink.terminate();
    }

    Map<Integer, Set<String>> partitionsById = new HashMap<>();
    int consumed = 0;
    try (EventHubConsumerClient consumer =
        new EventHubClientBuilder()
            .connectionString(eventHubs.getConnectionString(), KEYED_EVENT_HUB_NAME)
            .consumerGroup(CONSUMER_GROUP)
            .buildConsumerClient()) {
      for (String partitionId : consumer.getPartitionIds()) {
        for (PartitionEvent partitionEvent :
            consumer.receiveFromPartition(
                partitionId, events.size(), EventPosition.earliest(), Duration.ofSeconds(10))) {
          Map<String, Object> body =
              fromJsonString(partitionEvent.getData().getBodyAsString(), new TypeReference<>() {});
          int id = ((Number) body.get("id")).intValue();
          // The event carries the numeric id as its partition key ("4", not "4.0" and not absent).
          // Asserted per event because an unkeyed batch also lands wholly in one partition, so
          // co-location alone would not distinguish the fixed behavior from the bug.
          assertEquals(
              String.valueOf(id),
              partitionEvent.getData().getPartitionKey(),
              "event was published without its numeric partition key");
          partitionsById.computeIfAbsent(id, k -> new HashSet<>()).add(partitionId);
          consumed++;
        }
      }
    }

    assertEquals(events.size(), consumed, "not all events were read back");
    assertEquals(Set.of(1, 2, 3, 4), partitionsById.keySet());
    partitionsById.forEach(
        (id, partitions) ->
            assertEquals(1, partitions.size(), "id " + id + " was split across " + partitions));
  }

  private void publishEvents(int eventCount) throws Exception {
    AzureEventHubSinkDto.Config config =
        AzureEventHubSinkDto.Config.builder()
            .connectionString(eventHubs.getConnectionString())
            .eventHubName(EVENT_HUB_NAME)
            .encodingType("JSON_OBJECT")
            .build();

    AzureEventHubSinkCommand sink =
        (AzureEventHubSinkCommand)
            new AzureEventHubSinkCommandFactory().createCommand("sink", TestUtils.JOB_CONTEXT);
    sink.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());

    List<RecordFleakData> events = new ArrayList<>();
    for (int i = 0; i < eventCount; i++) {
      events.add((RecordFleakData) FleakData.wrap(Map.of("id", i)));
    }

    ScalarSinkCommand.SinkResult result =
        sink.writeToSink(events, "test_user", sink.getExecutionContext());
    assertEquals(eventCount, result.getSuccessCount());
    sink.terminate();
  }

  private AzureEventHubSourceCommand buildSourceCommand() {
    AzureEventHubSourceDto.Config config =
        AzureEventHubSourceDto.Config.builder()
            .connectionString(eventHubs.getConnectionString())
            .eventHubName(EVENT_HUB_NAME)
            .consumerGroup(CONSUMER_GROUP)
            .checkpointStorageConnectionString(azurite.getConnectionString())
            .checkpointContainerName(CHECKPOINT_CONTAINER)
            .encodingType(EncodingType.JSON_OBJECT)
            .initialPosition(AzureEventHubSourceDto.InitialPosition.EARLIEST)
            .commitStrategy(AzureEventHubSourceDto.CommitStrategyType.PER_RECORD)
            .build();

    AzureEventHubSourceCommand command =
        (AzureEventHubSourceCommand)
            new AzureEventHubSourceCommandFactory().createCommand("source", TestUtils.JOB_CONTEXT);
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    return command;
  }

  private boolean checkpointBlobWritten() {
    BlobContainerClient container =
        new BlobContainerClientBuilder()
            .connectionString(azurite.getConnectionString())
            .containerName(CHECKPOINT_CONTAINER)
            .buildClient();
    return StreamSupport.stream(container.listBlobs().spliterator(), false)
        .anyMatch(b -> b.getName().contains("checkpoint"));
  }

  private static class CollectingAcceptor implements SourceEventAcceptor {
    private final List<RecordFleakData> received = Collections.synchronizedList(new ArrayList<>());
    private final CountDownLatch latch;

    CollectingAcceptor(int expected) {
      this.latch = new CountDownLatch(expected);
    }

    @Override
    public void accept(List<RecordFleakData> records) {
      received.addAll(records);
      records.forEach(r -> latch.countDown());
    }

    @Override
    public void terminate() {}
  }
}
