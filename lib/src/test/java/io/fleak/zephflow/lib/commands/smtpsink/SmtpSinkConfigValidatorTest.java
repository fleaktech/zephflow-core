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
package io.fleak.zephflow.lib.commands.smtpsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SmtpSinkConfigValidatorTest {

  private SmtpSinkConfigValidator validator;
  private JobContext mockJobContext;

  @BeforeEach
  void setUp() {
    validator = new SmtpSinkConfigValidator();
    mockJobContext = mock(JobContext.class);
  }

  @Test
  void testValidConfig() {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.gmail.com")
            .port(587)
            .credentialId("smtp-cred")
            .fromAddress("noreply@example.com")
            .toTemplate("{{$.email}}")
            .subjectTemplate("Hello {{$.name}}")
            .bodyTemplate("Dear {{$.name}}, your order is ready.")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void testMissingHost() {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("")
            .credentialId("cred")
            .fromAddress("from@test.com")
            .toTemplate("to@test.com")
            .subjectTemplate("subj")
            .bodyTemplate("body")
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no host is provided"));
  }

  @Test
  void testNullHost() {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host(null)
            .credentialId("cred")
            .fromAddress("from@test.com")
            .toTemplate("to@test.com")
            .subjectTemplate("subj")
            .bodyTemplate("body")
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no host is provided"));
  }

  @Test
  void testInvalidPort() {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.example.com")
            .port(-1)
            .credentialId("cred")
            .fromAddress("from@test.com")
            .toTemplate("to@test.com")
            .subjectTemplate("subj")
            .bodyTemplate("body")
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("port must be positive"));
  }

  @Test
  void testMissingCredentialId() {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.example.com")
            .credentialId("")
            .fromAddress("from@test.com")
            .toTemplate("to@test.com")
            .subjectTemplate("subj")
            .bodyTemplate("body")
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no credentialId is provided"));
  }

  @Test
  void testMissingFromAddress() {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.example.com")
            .credentialId("cred")
            .fromAddress("")
            .toTemplate("to@test.com")
            .subjectTemplate("subj")
            .bodyTemplate("body")
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no fromAddress is provided"));
  }

  @Test
  void testMissingToTemplate() {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.example.com")
            .credentialId("cred")
            .fromAddress("from@test.com")
            .toTemplate("")
            .subjectTemplate("subj")
            .bodyTemplate("body")
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no toTemplate is provided"));
  }

  @Test
  void testMissingSubjectTemplate() {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.example.com")
            .credentialId("cred")
            .fromAddress("from@test.com")
            .toTemplate("to@test.com")
            .subjectTemplate("")
            .bodyTemplate("body")
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no subjectTemplate is provided"));
  }

  @Test
  void testMissingBodyTemplate() {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.example.com")
            .credentialId("cred")
            .fromAddress("from@test.com")
            .toTemplate("to@test.com")
            .subjectTemplate("subj")
            .bodyTemplate("")
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no bodyTemplate is provided"));
  }

  @Test
  void testDefaultValues() {
    SmtpSinkDto.Config config =
        SmtpSinkDto.Config.builder()
            .host("smtp.example.com")
            .credentialId("cred")
            .fromAddress("from@test.com")
            .toTemplate("to@test.com")
            .subjectTemplate("subj")
            .bodyTemplate("body")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
    assertEquals(587, config.getPort());
    assertEquals("text/plain", config.getBodyContentType());
    assertTrue(config.getUseTls());
  }
}
