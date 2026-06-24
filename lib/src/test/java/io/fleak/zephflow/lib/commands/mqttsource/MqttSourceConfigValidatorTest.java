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
package io.fleak.zephflow.lib.commands.mqttsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MqttSourceConfigValidatorTest {

  private MqttSourceConfigValidator validator;
  private JobContext mockJobContext;

  @BeforeEach
  void setUp() {
    validator = new MqttSourceConfigValidator();
    mockJobContext = mock(JobContext.class);
  }

  private MqttSourceDto.Config.ConfigBuilder validBuilder() {
    return MqttSourceDto.Config.builder()
        .brokerUrl("tcp://localhost:1883")
        .topicFilter("sensors/#")
        .clientId("my-client")
        .encodingType(EncodingType.JSON_OBJECT);
  }

  @Test
  void testValidConfig() {
    assertDoesNotThrow(
        () -> validator.validateConfig(validBuilder().build(), "test-node", mockJobContext));
  }

  @Test
  void testMissingBrokerUrl() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().brokerUrl("").build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no brokerUrl is provided"));
  }

  @Test
  void testMissingTopicFilter() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().topicFilter("").build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no topicFilter is provided"));
  }

  @Test
  void testMissingClientId() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().clientId("").build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no clientId is provided"));
  }

  @Test
  void testMissingEncodingType() {
    NullPointerException ex =
        assertThrows(
            NullPointerException.class,
            () ->
                validator.validateConfig(
                    validBuilder().encodingType(null).build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no encoding type is provided"));
  }

  @Test
  void testUnsupportedEncodingType() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().encodingType(EncodingType.PARQUET).build(),
                    "test-node",
                    mockJobContext));
    assertTrue(ex.getMessage().contains("Unsupported deserialization encoding type"));
  }

  @Test
  void testInvalidQos() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().qos(3).build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("qos must be 0, 1, or 2"));
  }

  @Test
  void testInvalidMaxBatchSize() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().maxBatchSize(0).build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("maxBatchSize must be positive"));
  }

  @Test
  void testInvalidReceiveQueueCapacity() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().receiveQueueCapacity(0).build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("receiveQueueCapacity must be positive"));
  }

  @Test
  void testInvalidReceiveTimeoutMs() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().receiveTimeoutMs(0L).build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("receiveTimeoutMs must be positive"));
  }
}
