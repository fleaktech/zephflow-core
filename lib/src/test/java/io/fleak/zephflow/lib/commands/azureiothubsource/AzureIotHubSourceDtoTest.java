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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.EncodingType;
import org.junit.jupiter.api.Test;

class AzureIotHubSourceDtoTest {

  @Test
  void buildsWithDefaults() {
    AzureIotHubSourceDto.Config config =
        AzureIotHubSourceDto.Config.builder()
            .connectionString("Endpoint=sb://ns/;SharedAccessKeyName=k;SharedAccessKey=v")
            .eventHubName("hub")
            .checkpointStorageConnectionString("UseDevelopmentStorage=true")
            .checkpointContainerName("checkpoints")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    assertEquals("$Default", config.getConsumerGroup());
    assertEquals(AzureIotHubSourceDto.InitialPosition.EARLIEST, config.getInitialPosition());
    assertEquals(1024, config.getMaxBufferedEvents());
    assertEquals(500, config.getMaxEventsPerFetch());
    assertEquals(AzureIotHubSourceDto.CommitStrategyType.BATCH, config.getCommitStrategy());
    assertEquals(1000, config.getCommitBatchSize());
    assertEquals(5000L, config.getCommitIntervalMs());
  }
}
