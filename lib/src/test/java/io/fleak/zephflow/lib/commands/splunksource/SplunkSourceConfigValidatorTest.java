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
package io.fleak.zephflow.lib.commands.splunksource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.TestUtils;
import org.junit.jupiter.api.Test;

class SplunkSourceConfigValidatorTest {

  @Test
  void testConfigValidation() {
    var validator = new SplunkSourceConfigValidator();

    var validConfig =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .build();

    assertDoesNotThrow(
        () -> validator.validateConfig(validConfig, "test-node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void testConfigValidationHttpScheme() {
    var validator = new SplunkSourceConfigValidator();

    var validConfig =
        SplunkSourceDto.Config.builder()
            .splunkUrl("http://splunk.example.com:8089")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .build();

    assertDoesNotThrow(
        () -> validator.validateConfig(validConfig, "test-node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void testConfigValidationMissingHost() {
    var validator = new SplunkSourceConfigValidator();

    var invalidConfig =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .build();

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(invalidConfig, "test-node", TestUtils.JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("must have a host"));
  }

  @Test
  void testConfigValidationMissingScheme() {
    var validator = new SplunkSourceConfigValidator();

    var invalidConfig =
        SplunkSourceDto.Config.builder()
            .splunkUrl("splunk.example.com:8089")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(invalidConfig, "test-node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void testConfigValidationInvalidScheme() {
    var validator = new SplunkSourceConfigValidator();

    var invalidConfig =
        SplunkSourceDto.Config.builder()
            .splunkUrl("ftp://splunk.example.com:8089")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .build();

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(invalidConfig, "test-node", TestUtils.JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("must have http or https scheme"));
  }

  @Test
  void testConfigValidationMissingPort() {
    var validator = new SplunkSourceConfigValidator();

    var invalidConfig =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .build();

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(invalidConfig, "test-node", TestUtils.JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("must have a port"));
  }

  @Test
  void testConfigValidationMissingUrl() {
    var validator = new SplunkSourceConfigValidator();

    var invalidConfig =
        SplunkSourceDto.Config.builder()
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(invalidConfig, "test-node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void testConfigValidationMissingSearchQuery() {
    var validator = new SplunkSourceConfigValidator();

    var invalidConfig =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .credentialId("splunk-cred")
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(invalidConfig, "test-node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void testConfigValidationMissingCredentialId() {
    var validator = new SplunkSourceConfigValidator();

    var invalidConfig =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main")
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(invalidConfig, "test-node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void testConfigValidationNegativeJobInitTimeout() {
    var validator = new SplunkSourceConfigValidator();

    var invalidConfig =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .jobInitTimeoutMs(-1L)
            .build();

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(invalidConfig, "test-node", TestUtils.JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("jobInitTimeoutMs must be non-negative"));
  }

  @Test
  void testConfigValidationZeroJobInitTimeoutAllowed() {
    var validator = new SplunkSourceConfigValidator();

    var validConfig =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main")
            .credentialId("splunk-cred")
            .jobInitTimeoutMs(0L)
            .build();

    assertDoesNotThrow(
        () -> validator.validateConfig(validConfig, "test-node", TestUtils.JOB_CONTEXT));
  }
}
