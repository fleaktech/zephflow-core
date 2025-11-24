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
package io.fleak.zephflow.lib.commands.kafkasource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaSourceConfigValidatorTest {

  private KafkaSourceConfigValidator validator;
  private JobContext mockJobContext;

  @BeforeEach
  void setUp() {
    validator = new KafkaSourceConfigValidator();
    mockJobContext = mock(JobContext.class);
  }

  @Test
  void testValidConfigWithDefaults() {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    // Should not throw any exception
    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void testValidConfigWithCustomCommitStrategy() {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .commitStrategy(KafkaSourceDto.CommitStrategyType.BATCH)
            .commitBatchSize(2000)
            .commitIntervalMs(10000L)
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void testInvalidBroker() {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(exception.getMessage().contains("no broker is provided"));
  }

  @Test
  void testInvalidTopic() {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(exception.getMessage().contains("no topic is provided"));
  }

  @Test
  void testInvalidGroupId() {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(exception.getMessage().contains("no consumer group Id is provided"));
  }

  @Test
  void testNullEncodingType() {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(null)
            .build();

    NullPointerException exception =
        assertThrows(
            NullPointerException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(exception.getMessage().contains("no encoding type is provided"));
  }

  @Test
  void testInvalidCommitBatchSizeZero() {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .commitStrategy(KafkaSourceDto.CommitStrategyType.BATCH)
            .commitBatchSize(0)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(exception.getMessage().contains("commitBatchSize must be positive"));
  }

  @Test
  void testInvalidCommitBatchSizeNegative() {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .commitStrategy(KafkaSourceDto.CommitStrategyType.BATCH)
            .commitBatchSize(-100)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(exception.getMessage().contains("commitBatchSize must be positive"));
  }

  @Test
  void testInvalidCommitIntervalZero() {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .commitStrategy(KafkaSourceDto.CommitStrategyType.BATCH)
            .commitIntervalMs(0L)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(exception.getMessage().contains("commitIntervalMs must be positive"));
  }

  @Test
  void testInvalidCommitIntervalNegative() {
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .commitStrategy(KafkaSourceDto.CommitStrategyType.BATCH)
            .commitIntervalMs(-1000L)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(exception.getMessage().contains("commitIntervalMs must be positive"));
  }

  @Test
  void testPerRecordStrategyIgnoresCommitParams() {
    // Per-record strategy should not validate batch-specific parameters
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .commitStrategy(KafkaSourceDto.CommitStrategyType.PER_RECORD)
            .commitBatchSize(-1) // This should be ignored for PER_RECORD strategy
            .commitIntervalMs(-1L) // This should be ignored for PER_RECORD strategy
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void testNoneStrategyIgnoresCommitParams() {
    // None strategy should not validate batch-specific parameters
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .commitStrategy(KafkaSourceDto.CommitStrategyType.NONE)
            .commitBatchSize(-1) // This should be ignored for NONE strategy
            .commitIntervalMs(-1L) // This should be ignored for NONE strategy
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }
}
