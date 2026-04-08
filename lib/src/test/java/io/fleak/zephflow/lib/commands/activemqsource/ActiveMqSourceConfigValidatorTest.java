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
import io.fleak.zephflow.lib.serdes.EncodingType;
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
            .encodingType(EncodingType.JSON_OBJECT)
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
            .encodingType(EncodingType.JSON_OBJECT)
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
            .encodingType(EncodingType.JSON_OBJECT)
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
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    NullPointerException ex =
        assertThrows(
            NullPointerException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no brokerType is provided"));
  }

  @Test
  void testMissingDestination() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no destination is provided"));
  }

  @Test
  void testMissingEncodingType() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("test-queue")
            .encodingType(null)
            .build();

    NullPointerException ex =
        assertThrows(
            NullPointerException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no encoding type is provided"));
  }

  @Test
  void testTopicWithoutClientId() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("test-topic")
            .destinationType(ActiveMqSourceDto.DestinationType.TOPIC)
            .encodingType(EncodingType.JSON_OBJECT)
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
            .encodingType(EncodingType.JSON_OBJECT)
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
            .encodingType(EncodingType.JSON_OBJECT)
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
            .encodingType(EncodingType.JSON_OBJECT)
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
            .encodingType(EncodingType.JSON_OBJECT)
            .commitStrategy(ActiveMqSourceDto.CommitStrategyType.PER_RECORD)
            .commitBatchSize(-1)
            .commitIntervalMs(-1L)
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void testUnsupportedEncodingType() {
    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:61616")
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination("test-queue")
            .encodingType(EncodingType.PARQUET)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("Unsupported deserialization encoding type"));
  }
}
