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
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
 * End-to-end test of the IoT Hub source against the Event Hubs emulator (the IoT Hub built-in
 * endpoint is Event Hub-compatible) plus Azurite for checkpoint storage. Requires Docker.
 */
@Slf4j
@Tag("integration")
@Testcontainers
public class AzureIotHubSourceIntegrationTest {

  private static final String EVENT_HUB_NAME = "eh1";
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

    new BlobContainerClientBuilder()
        .connectionString(azurite.getConnectionString())
        .containerName(CHECKPOINT_CONTAINER)
        .buildClient()
        .createIfNotExists();
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
  void surfacesApplicationPropertiesUnderIothubEnvelope() throws Exception {
    publishTelemetryWithProperties();

    CollectingAcceptor acceptor = new CollectingAcceptor(1);
    AzureIotHubSourceCommand sourceCommand = buildSourceCommand();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(
          () -> {
            sourceCommand.initialize(new MetricClientProvider.NoopMetricClientProvider());
            sourceCommand.execute("test_user", acceptor);
          });

      assertTrue(
          acceptor.latch.await(90, TimeUnit.SECONDS), "did not receive the event within timeout");

      Map<String, Object> record = acceptor.received.get(0).unwrap();
      assertEquals(72.0, ((Number) record.get("temp")).doubleValue());

      @SuppressWarnings("unchecked")
      Map<String, Object> iothub = (Map<String, Object>) record.get("_iothub");
      assertNotNull(iothub, "expected _iothub envelope");
      assertNotNull(iothub.get("enqueuedTime"), "expected enqueuedTime in _iothub");

      @SuppressWarnings("unchecked")
      Map<String, Object> properties = (Map<String, Object>) iothub.get("properties");
      assertNotNull(properties, "expected application properties in _iothub");
      assertEquals("v2", properties.get("schema"));

      // Checkpoint path exercised end-to-end: a checkpoint blob was written to Azurite.
      assertTrue(checkpointBlobWritten(), "expected a checkpoint blob to be written to Azurite");
    } finally {
      sourceCommand.terminate();
      executor.shutdownNow();
      //noinspection ResultOfMethodCallIgnored
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  private void publishTelemetryWithProperties() {
    try (EventHubProducerClient producer =
        new EventHubClientBuilder()
            .connectionString(eventHubs.getConnectionString(), EVENT_HUB_NAME)
            .buildProducerClient()) {
      EventData event = new EventData("{\"temp\":72}".getBytes(StandardCharsets.UTF_8));
      event.getProperties().put("schema", "v2");
      producer.send(List.of(event));
    }
  }

  private AzureIotHubSourceCommand buildSourceCommand() {
    AzureIotHubSourceDto.Config config =
        AzureIotHubSourceDto.Config.builder()
            .connectionString(eventHubs.getConnectionString())
            .eventHubName(EVENT_HUB_NAME)
            .consumerGroup(CONSUMER_GROUP)
            .checkpointStorageConnectionString(azurite.getConnectionString())
            .checkpointContainerName(CHECKPOINT_CONTAINER)
            .encodingType(EncodingType.JSON_OBJECT)
            .initialPosition(AzureIotHubSourceDto.InitialPosition.EARLIEST)
            .commitStrategy(AzureIotHubSourceDto.CommitStrategyType.PER_RECORD)
            .build();

    AzureIotHubSourceCommand command =
        (AzureIotHubSourceCommand)
            new AzureIotHubSourceCommandFactory().createCommand("source", TestUtils.JOB_CONTEXT);
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    return command;
  }

  /**
   * Polls Azurite for a checkpoint blob. The checkpoint write happens on the source's commit path
   * just after an event is delivered to the acceptor, so it can lag the latch that releases this
   * assertion; poll with a bounded timeout rather than checking once.
   */
  private boolean checkpointBlobWritten() throws InterruptedException {
    BlobContainerClient container =
        new BlobContainerClientBuilder()
            .connectionString(azurite.getConnectionString())
            .containerName(CHECKPOINT_CONTAINER)
            .buildClient();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      boolean found =
          StreamSupport.stream(container.listBlobs().spliterator(), false)
              .anyMatch(b -> b.getName().contains("checkpoint"));
      if (found) {
        return true;
      }
      TimeUnit.MILLISECONDS.sleep(500);
    }
    return false;
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
