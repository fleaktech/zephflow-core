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

import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.encoding;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport;
import org.junit.jupiter.api.Test;

class AzureEventHubSinkConfigValidatorTest {

  private final AzureEventHubSinkConfigValidator validator = new AzureEventHubSinkConfigValidator();
  private final JobContext jobContext = mock(JobContext.class);

  private static AzureEventHubSinkDto.Config.ConfigBuilder validConfig() {
    return AzureEventHubSinkDto.Config.builder()
        .connectionString("Endpoint=sb://ns/;SharedAccessKeyName=k;SharedAccessKey=v")
        .eventHubName("hub")
        .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"));
  }

  private void validate(AzureEventHubSinkDto.Config config) {
    validator.validateConfig(config, "node", jobContext);
  }

  @Test
  void acceptsValidConfig() {
    assertDoesNotThrow(() -> validate(validConfig().build()));
  }

  @Test
  void acceptsValidPartitionKeyExpression() {
    assertDoesNotThrow(
        () -> validate(validConfig().partitionKeyFieldExpressionStr("$.id").build()));
  }

  @Test
  void rejectsMissingEventHubName() {
    var config = validConfig().eventHubName("").build();
    assertTrue(
        assertThrows(IllegalArgumentException.class, () -> validate(config))
            .getMessage()
            .contains("eventHubName"));
  }

  @Test
  void rejectsMissingAuth() {
    var config = validConfig().connectionString(null).build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsUnsupportedEncoding() {
    var config = validConfig().encoding(EncodeTestSupport.format("{\"type\":\"text\"}")).build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsInvalidPartitionKeyExpression() {
    var config = validConfig().partitionKeyFieldExpressionStr("((not-a-path").build();
    assertThrows(Exception.class, () -> validate(config));
  }

  @Test
  void acceptsEncodingAlone() {
    var config =
        validConfig()
            .encoding(encoding("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}"))
            .build();
    assertDoesNotThrow(() -> validate(config));
  }

  @Test
  void rejectsEncodingInAFormatItCannotWrite() {
    var config = validConfig().encoding(encoding("{\"format\":{\"type\":\"kv\"}}")).build();
    assertEquals(
        "azureeventhubsink cannot write kv",
        assertThrows(IllegalArgumentException.class, () -> validate(config)).getMessage());
  }

  @Test
  void rejectsMissingEncoding() {
    var config = validConfig().encoding(null).build();
    assertEquals(
        "azureeventhubsink: encoding is required",
        assertThrows(IllegalArgumentException.class, () -> validate(config)).getMessage());
  }
}
