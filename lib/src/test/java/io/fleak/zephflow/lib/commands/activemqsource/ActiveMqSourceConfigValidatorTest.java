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
package io.fleak.zephflow.lib.commands.activemqsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.filedecode.Decodings;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActiveMqSourceConfigValidatorTest {

  private ActiveMqSourceConfigValidator validator;
  private JobContext mockJobContext;

  @BeforeEach
  void setUp() {
    validator = new ActiveMqSourceConfigValidator();
    mockJobContext = mock(JobContext.class);
  }

  @Test
  void testValidQueueConfig() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("test-queue")
            .decoding(Decodings.uncompressed(Decodings.JSON_OBJECT))
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void testValidTopicConfig() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("test-topic")
            .destinationType(ActiveMqSourceDto.DestinationType.TOPIC)
            .decoding(Decodings.uncompressed(Decodings.JSON_OBJECT))
            .clientId("my-client")
            .subscriptionName("my-sub")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void testMissingBrokerUrl() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("test-queue")
            .decoding(Decodings.uncompressed(Decodings.JSON_OBJECT))
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no brokerUrl is provided"));
  }

  @Test
  void testMissingBrokerType() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(null)
            .destination("test-queue")
            .decoding(Decodings.uncompressed(Decodings.JSON_OBJECT))
            .build();

    io.fleak.zephflow.api.ConfigurationValidationException ex =
        assertThrows(
            io.fleak.zephflow.api.ConfigurationValidationException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertEquals(NullPointerException.class, ex.originalFailure().getClass());
    assertEquals(
        io.fleak.zephflow.api.ConfigurationPath.of("brokerType"), ex.issues().getFirst().path());
    assertTrue(ex.getMessage().contains("no brokerType is provided"));
  }

  @Test
  void testMissingDestination() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("")
            .decoding(Decodings.uncompressed(Decodings.JSON_OBJECT))
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no destination is provided"));
  }

  @Test
  void testTopicWithoutClientId() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("test-topic")
            .destinationType(ActiveMqSourceDto.DestinationType.TOPIC)
            .decoding(Decodings.uncompressed(Decodings.JSON_OBJECT))
            .subscriptionName("my-sub")
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("clientId is required"));
  }

  @Test
  void testTopicWithoutSubscriptionName() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("test-topic")
            .destinationType(ActiveMqSourceDto.DestinationType.TOPIC)
            .decoding(Decodings.uncompressed(Decodings.JSON_OBJECT))
            .clientId("my-client")
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("subscriptionName is required"));
  }

  @Test
  void testInvalidCommitBatchSize() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("test-queue")
            .decoding(Decodings.uncompressed(Decodings.JSON_OBJECT))
            .commitStrategy(ActiveMqSourceDto.CommitStrategyType.BATCH)
            .commitBatchSize(0)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("commitBatchSize must be positive"));
  }

  @Test
  void testInvalidCommitIntervalMs() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("test-queue")
            .decoding(Decodings.uncompressed(Decodings.JSON_OBJECT))
            .commitStrategy(ActiveMqSourceDto.CommitStrategyType.BATCH)
            .commitIntervalMs(-1L)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("commitIntervalMs must be positive"));
  }

  @Test
  void testPerRecordStrategyIgnoresCommitParams() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("test-queue")
            .decoding(Decodings.uncompressed(Decodings.JSON_OBJECT))
            .commitStrategy(ActiveMqSourceDto.CommitStrategyType.PER_RECORD)
            .commitBatchSize(-1)
            .commitIntervalMs(-1L)
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void missingDecodingIsRejected() {
    ActiveMqSourceDto.Config config = decodingBase().build();

    assertEquals("decoding is required", decodingRejection(config));
  }

  @Test
  void decodingIsAccepted() {
    ActiveMqSourceDto.Config config =
        decodingBase()
            .decoding(
                new DecodingConfig(
                    List.of(DecompressionCodec.ZSTD),
                    new JsonFormatConfig(JsonFormatConfig.Mode.LINES)))
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void aDecodingTheCompilerRejectsFailsValidation() {
    ActiveMqSourceDto.Config config =
        decodingBase()
            .decoding(
                new DecodingConfig(List.of(DecompressionCodec.GZIP), new ParquetFormatConfig()))
            .build();

    assertEquals(
        "parquet cannot be combined with compression; its pages carry their own",
        decodingRejection(config));
  }

  private String decodingRejection(ActiveMqSourceDto.Config config) {
    return assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext))
        .getMessage();
  }

  private static ActiveMqSourceDto.Config.ConfigBuilder decodingBase() {
    return ActiveMqSourceDto.Config.builder()
        .brokerUrl("tcp://localhost:61616")
        .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
        .destination("test-queue");
  }
}
