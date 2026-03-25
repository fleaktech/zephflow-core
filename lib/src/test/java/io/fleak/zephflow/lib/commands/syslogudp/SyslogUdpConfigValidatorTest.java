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
package io.fleak.zephflow.lib.commands.syslogudp;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.TestUtils;
import org.junit.jupiter.api.Test;

class SyslogUdpConfigValidatorTest {

  private final SyslogUdpConfigValidator validator = new SyslogUdpConfigValidator();

  @Test
  void validConfig() {
    SyslogUdpDto.Config config = SyslogUdpDto.Config.builder().host("0.0.0.0").port(514).build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "test_node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void invalidPort_zero() {
    SyslogUdpDto.Config config = SyslogUdpDto.Config.builder().host("0.0.0.0").port(0).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "test_node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void invalidPort_negative() {
    SyslogUdpDto.Config config = SyslogUdpDto.Config.builder().host("0.0.0.0").port(-1).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "test_node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void invalidPort_tooHigh() {
    SyslogUdpDto.Config config = SyslogUdpDto.Config.builder().host("0.0.0.0").port(65536).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "test_node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void invalidBufferSize() {
    SyslogUdpDto.Config config =
        SyslogUdpDto.Config.builder().host("0.0.0.0").port(514).bufferSize(0).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "test_node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void invalidQueueCapacity() {
    SyslogUdpDto.Config config =
        SyslogUdpDto.Config.builder().host("0.0.0.0").port(514).queueCapacity(0).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "test_node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void invalidEncoding() {
    SyslogUdpDto.Config config =
        SyslogUdpDto.Config.builder().host("0.0.0.0").port(514).encoding("INVALID_CHARSET").build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "test_node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void blankHost() {
    SyslogUdpDto.Config config = SyslogUdpDto.Config.builder().host("").port(514).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "test_node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void defaultValues() {
    SyslogUdpDto.Config config = SyslogUdpDto.Config.builder().build();
    assertEquals("0.0.0.0", config.getHost());
    assertEquals(514, config.getPort());
    assertEquals(65535, config.getBufferSize());
    assertEquals(10000, config.getQueueCapacity());
    assertEquals("UTF-8", config.getEncoding());
  }
}
