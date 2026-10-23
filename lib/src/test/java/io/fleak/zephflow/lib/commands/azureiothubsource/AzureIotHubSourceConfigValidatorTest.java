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
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import java.util.List;
import org.junit.jupiter.api.Test;

class AzureIotHubSourceConfigValidatorTest {

  private final AzureIotHubSourceConfigValidator validator = new AzureIotHubSourceConfigValidator();
  private final JobContext jobContext = mock(JobContext.class);

  private static AzureIotHubSourceDto.Config.ConfigBuilder validConnectionStringConfig() {
    return AzureIotHubSourceDto.Config.builder()
        .connectionString("Endpoint=sb://ns/;SharedAccessKeyName=k;SharedAccessKey=v")
        .eventHubName("hub")
        .consumerGroup("$Default")
        .checkpointStorageConnectionString("UseDevelopmentStorage=true")
        .checkpointContainerName("checkpoints")
        .encodingType(EncodingType.JSON_OBJECT);
  }

  private void validate(AzureIotHubSourceDto.Config config) {
    validator.validateConfig(config, "node", jobContext);
  }

  @Test
  void acceptsValidConnectionStringConfig() {
    assertDoesNotThrow(() -> validate(validConnectionStringConfig().build()));
  }

  @Test
  void acceptsValidEntraIdConfig() {
    AzureIotHubSourceDto.Config config =
        validConnectionStringConfig()
            .connectionString(null)
            .fullyQualifiedNamespace("ns.servicebus.windows.net")
            .tenantId("t")
            .clientId("c")
            .clientSecret("s")
            .build();
    assertDoesNotThrow(() -> validate(config));
  }

  @Test
  void rejectsMissingEventHubName() {
    var config = validConnectionStringConfig().eventHubName(" ").build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsWhenNoAuthProvided() {
    var config = validConnectionStringConfig().connectionString(null).build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsWhenBothAuthModesProvided() {
    var config =
        validConnectionStringConfig().fullyQualifiedNamespace("ns.servicebus.windows.net").build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsMissingCheckpointContainer() {
    var config = validConnectionStringConfig().checkpointContainerName(null).build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsBothCheckpointStorageAuthModes() {
    var config =
        validConnectionStringConfig().checkpointStorageEndpoint("https://acct.blob.core").build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void neitherEncodingTypeNorDecodingIsRejected() {
    var config = validConnectionStringConfig().encodingType(null).build();

    assertEquals("exactly one of encodingType and decoding must be set", decodingRejection(config));
  }

  @Test
  void decodingReplacesEncodingType() {
    var config =
        validConnectionStringConfig()
            .encodingType(null)
            .decoding(
                new DecodingConfig(
                    List.of(DecompressionCodec.ZSTD),
                    new JsonFormatConfig(JsonFormatConfig.Mode.LINES)))
            .build();

    assertDoesNotThrow(() -> validate(config));
  }

  @Test
  void encodingTypeAndDecodingTogetherAreRejected() {
    var config =
        validConnectionStringConfig()
            .decoding(new DecodingConfig(List.of(), new ParquetFormatConfig()))
            .build();

    assertEquals("exactly one of encodingType and decoding must be set", decodingRejection(config));
  }

  @Test
  void aDecodingTheCompilerRejectsFailsValidation() {
    var config =
        validConnectionStringConfig()
            .encodingType(null)
            .decoding(
                new DecodingConfig(List.of(DecompressionCodec.GZIP), new ParquetFormatConfig()))
            .build();

    assertEquals(
        "parquet cannot be combined with compression; its pages carry their own",
        decodingRejection(config));
  }

  @Test
  void rejectsNonPositiveBufferSizes() {
    assertThrows(
        IllegalArgumentException.class,
        () -> validate(validConnectionStringConfig().maxBufferedEvents(0).build()));
  }

  private String decodingRejection(AzureIotHubSourceDto.Config config) {
    return assertThrows(IllegalArgumentException.class, () -> validate(config)).getMessage();
  }
}
