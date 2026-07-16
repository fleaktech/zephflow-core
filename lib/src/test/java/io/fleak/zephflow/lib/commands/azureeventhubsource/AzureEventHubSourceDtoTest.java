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
package io.fleak.zephflow.lib.commands.azureeventhubsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AzureEventHubSourceDtoTest {

  @Test
  void appliesSensibleDefaults() {
    AzureEventHubSourceDto.Config config =
        AzureEventHubSourceDto.Config.builder()
            .connectionString("cs")
            .eventHubName("hub")
            .checkpointStorageConnectionString("scs")
            .checkpointContainerName("c")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    assertEquals("$Default", config.getConsumerGroup());
    assertEquals(AzureEventHubSourceDto.InitialPosition.EARLIEST, config.getInitialPosition());
    assertEquals(AzureEventHubSourceDto.CommitStrategyType.BATCH, config.getCommitStrategy());
    assertEquals(1000, config.getCommitBatchSize());
    assertEquals(5000L, config.getCommitIntervalMs());
    assertEquals(1024, config.getMaxBufferedEvents());
    assertEquals(500, config.getMaxEventsPerFetch());
  }

  @Test
  void parsesFromJsonMap() {
    Map<String, Object> json =
        Map.of(
            "fullyQualifiedNamespace", "ns.servicebus.windows.net",
            "eventHubName", "hub",
            "consumerGroup", "cg",
            "checkpointStorageEndpoint", "https://acct.blob.core.windows.net",
            "checkpointContainerName", "checkpoints",
            "encodingType", "JSON_OBJECT",
            "initialPosition", "LATEST",
            "maxEventsPerFetch", 250);

    AzureEventHubSourceDto.Config config =
        OBJECT_MAPPER.convertValue(json, AzureEventHubSourceDto.Config.class);

    assertEquals("ns.servicebus.windows.net", config.getFullyQualifiedNamespace());
    assertEquals("hub", config.getEventHubName());
    assertEquals("cg", config.getConsumerGroup());
    assertEquals("checkpoints", config.getCheckpointContainerName());
    assertEquals(EncodingType.JSON_OBJECT, config.getEncodingType());
    assertEquals(AzureEventHubSourceDto.InitialPosition.LATEST, config.getInitialPosition());
    assertEquals(250, config.getMaxEventsPerFetch());
  }
}
