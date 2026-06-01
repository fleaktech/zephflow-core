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
package io.fleak.zephflow.lib.commands.oktasource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OktaSourceConfigValidatorTest {

  private OktaSourceConfigValidator validator;
  private JobContext mockJobContext;

  @BeforeEach
  void setUp() {
    validator = new OktaSourceConfigValidator();
    mockJobContext = mock(JobContext.class);
  }

  @Test
  void acceptsValidConfig() {
    OktaSourceDto.Config config =
        OktaSourceDto.Config.builder()
            .oktaDomain("mycompany.okta.com")
            .credentialId("okta-cred")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void acceptsConfigWithOptionalFields() {
    OktaSourceDto.Config config =
        OktaSourceDto.Config.builder()
            .oktaDomain("mycompany.okta.com")
            .credentialId("okta-cred")
            .sinceTimestamp("2026-01-01T00:00:00Z")
            .filter("eventType eq \"user.session.start\"")
            .limit(500)
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
  }

  @Test
  void rejectsBlankOktaDomain() {
    OktaSourceDto.Config config =
        OktaSourceDto.Config.builder().oktaDomain("").credentialId("okta-cred").build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("oktaDomain is required"));
  }

  @Test
  void rejectsBlankCredentialId() {
    OktaSourceDto.Config config =
        OktaSourceDto.Config.builder().oktaDomain("mycompany.okta.com").credentialId("").build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("credentialId is required"));
  }

  @Test
  void rejectsLimitBelowMinimum() {
    OktaSourceDto.Config config =
        OktaSourceDto.Config.builder()
            .oktaDomain("mycompany.okta.com")
            .credentialId("okta-cred")
            .limit(0)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("limit must be between"));
  }

  @Test
  void rejectsLimitAboveMaximum() {
    OktaSourceDto.Config config =
        OktaSourceDto.Config.builder()
            .oktaDomain("mycompany.okta.com")
            .credentialId("okta-cred")
            .limit(OktaSourceDto.MAX_LIMIT + 1)
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("limit must be between"));
  }

  @Test
  void appliesDefaultLimit() {
    OktaSourceDto.Config config =
        OktaSourceDto.Config.builder()
            .oktaDomain("mycompany.okta.com")
            .credentialId("okta-cred")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", mockJobContext));
    assertEquals(OktaSourceDto.DEFAULT_LIMIT, config.getLimit());
  }
}
