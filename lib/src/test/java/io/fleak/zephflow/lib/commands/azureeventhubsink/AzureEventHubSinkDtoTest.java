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
package io.fleak.zephflow.lib.commands.azureeventhubsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class AzureEventHubSinkDtoTest {

  @Test
  void buildsWithAllFields() {
    AzureEventHubSinkDto.Config config =
        AzureEventHubSinkDto.Config.builder()
            .connectionString("cs")
            .eventHubName("hub")
            .partitionKeyFieldExpressionStr("$.id")
            .encodingType("JSON_OBJECT")
            .build();

    assertEquals("cs", config.getConnectionString());
    assertEquals("hub", config.getEventHubName());
    assertEquals("$.id", config.getPartitionKeyFieldExpressionStr());
    assertEquals("JSON_OBJECT", config.getEncodingType());
  }

  @Test
  void parsesFromJsonMap() {
    Map<String, Object> json =
        Map.of(
            "fullyQualifiedNamespace", "ns.servicebus.windows.net",
            "eventHubName", "hub",
            "tenantId", "t",
            "clientId", "c",
            "clientSecret", "s",
            "encodingType", "JSON_OBJECT");

    AzureEventHubSinkDto.Config config =
        OBJECT_MAPPER.convertValue(json, AzureEventHubSinkDto.Config.class);

    assertEquals("ns.servicebus.windows.net", config.getFullyQualifiedNamespace());
    assertEquals("hub", config.getEventHubName());
    assertEquals("t", config.getTenantId());
    assertEquals("JSON_OBJECT", config.getEncodingType());
    assertNull(config.getConnectionString());
  }
}
