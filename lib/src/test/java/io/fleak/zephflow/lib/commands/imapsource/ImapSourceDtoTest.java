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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ImapSourceDtoTest {

  @Test
  void testConfigParsing() {
    Map<String, Object> configMap =
        Map.of(
            "host",
            "imap.example.com",
            "port",
            993,
            "credentialId",
            "email-cred",
            "authType",
            "PASSWORD",
            "folder",
            "INBOX",
            "searchCriteria",
            "UNSEEN",
            "pollIntervalMs",
            30000,
            "markAsRead",
            true,
            "includeAttachments",
            false,
            "useSsl",
            true);

    ImapSourceDto.Config config = OBJECT_MAPPER.convertValue(configMap, ImapSourceDto.Config.class);

    assertEquals("imap.example.com", config.getHost());
    assertEquals(993, config.getPort());
    assertEquals("email-cred", config.getCredentialId());
    assertEquals(ImapSourceDto.AuthType.PASSWORD, config.getAuthType());
    assertEquals("INBOX", config.getFolder());
    assertEquals("UNSEEN", config.getSearchCriteria());
    assertEquals(30000L, config.getPollIntervalMs());
    assertTrue(config.getMarkAsRead());
    assertFalse(config.getIncludeAttachments());
    assertTrue(config.getUseSsl());
  }

  @Test
  void testConfigParsingWithDefaults() {
    Map<String, Object> configMap =
        Map.of(
            "host", "imap.example.com",
            "credentialId", "email-cred");

    ImapSourceDto.Config config = OBJECT_MAPPER.convertValue(configMap, ImapSourceDto.Config.class);

    assertEquals("imap.example.com", config.getHost());
    assertEquals(ImapSourceDto.DEFAULT_PORT, config.getPort());
    assertEquals(ImapSourceDto.AuthType.PASSWORD, config.getAuthType());
    assertEquals(ImapSourceDto.DEFAULT_FOLDER, config.getFolder());
    assertNull(config.getSearchCriteria());
    assertEquals(ImapSourceDto.DEFAULT_POLL_INTERVAL_MS, config.getPollIntervalMs());
    assertEquals(ImapSourceDto.DEFAULT_MARK_AS_READ, config.getMarkAsRead());
    assertEquals(ImapSourceDto.DEFAULT_INCLUDE_ATTACHMENTS, config.getIncludeAttachments());
    assertEquals(ImapSourceDto.DEFAULT_USE_SSL, config.getUseSsl());
    assertEquals(ImapSourceDto.DEFAULT_MAX_MESSAGES, config.getMaxMessages());
  }

  @Test
  void testConfigParsingOAuth2() {
    Map<String, Object> configMap =
        Map.of(
            "host", "imap.gmail.com",
            "credentialId", "oauth-cred",
            "authType", "OAUTH2");

    ImapSourceDto.Config config = OBJECT_MAPPER.convertValue(configMap, ImapSourceDto.Config.class);

    assertEquals(ImapSourceDto.AuthType.OAUTH2, config.getAuthType());
  }

  @Test
  void testConfigBuilder() {
    ImapSourceDto.Config config =
        ImapSourceDto.Config.builder()
            .host("imap.gmail.com")
            .port(993)
            .credentialId("test-cred")
            .authType(ImapSourceDto.AuthType.PASSWORD)
            .folder("Sent")
            .searchCriteria("UNSEEN")
            .pollIntervalMs(5000L)
            .markAsRead(false)
            .includeAttachments(true)
            .useSsl(true)
            .maxMessages(50)
            .build();

    assertEquals("imap.gmail.com", config.getHost());
    assertEquals(993, config.getPort());
    assertEquals("Sent", config.getFolder());
    assertEquals(50, config.getMaxMessages());
    assertFalse(config.getMarkAsRead());
    assertTrue(config.getIncludeAttachments());
  }
}
