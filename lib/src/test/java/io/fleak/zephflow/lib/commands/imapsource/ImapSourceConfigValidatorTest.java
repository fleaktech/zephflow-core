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
package io.fleak.zephflow.lib.commands.imapsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ImapSourceConfigValidatorTest {

  private ImapSourceConfigValidator validator;
  private JobContext mockJobContext;

  @BeforeEach
  void setUp() {
    validator = new ImapSourceConfigValidator();
    mockJobContext = mock(JobContext.class);
  }

  @Test
  void testValidConfig() {
    ImapSourceDto.Config config =
        ImapSourceDto.Config.builder()
            .host("imap.gmail.com")
            .port(993)
            .credentialId("my-cred")
            .authType(ImapSourceDto.AuthType.PASSWORD)
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void testValidOAuth2Config() {
    ImapSourceDto.Config config =
        ImapSourceDto.Config.builder()
            .host("imap.gmail.com")
            .port(993)
            .credentialId("my-oauth-cred")
            .authType(ImapSourceDto.AuthType.OAUTH2)
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void testMissingHost() {
    ImapSourceDto.Config config =
        ImapSourceDto.Config.builder()
            .host("")
            .credentialId("my-cred")
            .authType(ImapSourceDto.AuthType.PASSWORD)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no host is provided"));
  }

  @Test
  void testNullHost() {
    ImapSourceDto.Config config =
        ImapSourceDto.Config.builder()
            .host(null)
            .credentialId("my-cred")
            .authType(ImapSourceDto.AuthType.PASSWORD)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no host is provided"));
  }

  @Test
  void testInvalidPort() {
    ImapSourceDto.Config config =
        ImapSourceDto.Config.builder()
            .host("imap.gmail.com")
            .port(-1)
            .credentialId("my-cred")
            .authType(ImapSourceDto.AuthType.PASSWORD)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("port must be positive"));
  }

  @Test
  void testMissingCredentialId() {
    ImapSourceDto.Config config =
        ImapSourceDto.Config.builder()
            .host("imap.gmail.com")
            .credentialId("")
            .authType(ImapSourceDto.AuthType.PASSWORD)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no credentialId is provided"));
  }

  @Test
  void testNullAuthType() {
    ImapSourceDto.Config config =
        ImapSourceDto.Config.builder()
            .host("imap.gmail.com")
            .credentialId("my-cred")
            .authType(null)
            .build();

    NullPointerException ex =
        assertThrows(
            NullPointerException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no authType is provided"));
  }

  @Test
  void testInvalidMaxMessages() {
    ImapSourceDto.Config config =
        ImapSourceDto.Config.builder()
            .host("imap.gmail.com")
            .credentialId("my-cred")
            .authType(ImapSourceDto.AuthType.PASSWORD)
            .maxMessages(0)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("maxMessages must be positive"));
  }

  @Test
  void testInvalidPollIntervalMs() {
    ImapSourceDto.Config config =
        ImapSourceDto.Config.builder()
            .host("imap.gmail.com")
            .credentialId("my-cred")
            .authType(ImapSourceDto.AuthType.PASSWORD)
            .pollIntervalMs(-1L)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("pollIntervalMs must be positive"));
  }

  @Test
  void testDefaultValues() {
    ImapSourceDto.Config config =
        ImapSourceDto.Config.builder().host("imap.gmail.com").credentialId("my-cred").build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
    assertEquals(993, config.getPort());
    assertEquals("INBOX", config.getFolder());
    assertEquals(60000L, config.getPollIntervalMs());
    assertTrue(config.getMarkAsRead());
    assertFalse(config.getIncludeAttachments());
    assertTrue(config.getUseSsl());
    assertEquals(100, config.getMaxMessages());
  }
}
