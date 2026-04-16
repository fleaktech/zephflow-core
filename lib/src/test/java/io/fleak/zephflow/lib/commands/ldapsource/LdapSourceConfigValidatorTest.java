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
package io.fleak.zephflow.lib.commands.ldapsource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.TestUtils;
import org.junit.jupiter.api.Test;

class LdapSourceConfigValidatorTest {

  private final LdapSourceConfigValidator validator = new LdapSourceConfigValidator();

  @Test
  void testValidConfig() {
    var config =
        LdapSourceDto.Config.builder()
            .ldapUrl("ldap://ldap.example.com:389")
            .credentialId("ldap-cred")
            .baseDn("ou=users,dc=example,dc=com")
            .searchFilter("(objectClass=person)")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void testValidConfigLdaps() {
    var config =
        LdapSourceDto.Config.builder()
            .ldapUrl("ldaps://ldap.example.com:636")
            .credentialId("ldap-cred")
            .baseDn("ou=users,dc=example,dc=com")
            .searchFilter("(objectClass=person)")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void testMissingLdapUrl() {
    var config =
        LdapSourceDto.Config.builder()
            .credentialId("ldap-cred")
            .baseDn("ou=users,dc=example,dc=com")
            .searchFilter("(objectClass=person)")
            .build();

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("ldapUrl is required"));
  }

  @Test
  void testBlankLdapUrl() {
    var config =
        LdapSourceDto.Config.builder()
            .ldapUrl("  ")
            .credentialId("ldap-cred")
            .baseDn("ou=users,dc=example,dc=com")
            .searchFilter("(objectClass=person)")
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void testInvalidUrlScheme() {
    var config =
        LdapSourceDto.Config.builder()
            .ldapUrl("http://ldap.example.com:389")
            .credentialId("ldap-cred")
            .baseDn("ou=users,dc=example,dc=com")
            .searchFilter("(objectClass=person)")
            .build();

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("must start with ldap:// or ldaps://"));
  }

  @Test
  void testMissingBaseDn() {
    var config =
        LdapSourceDto.Config.builder()
            .ldapUrl("ldap://ldap.example.com:389")
            .credentialId("ldap-cred")
            .searchFilter("(objectClass=person)")
            .build();

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("baseDn is required"));
  }

  @Test
  void testMissingSearchFilter() {
    var config =
        LdapSourceDto.Config.builder()
            .ldapUrl("ldap://ldap.example.com:389")
            .credentialId("ldap-cred")
            .baseDn("ou=users,dc=example,dc=com")
            .build();

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("searchFilter is required"));
  }

  @Test
  void testMissingCredentialId() {
    var config =
        LdapSourceDto.Config.builder()
            .ldapUrl("ldap://ldap.example.com:389")
            .baseDn("ou=users,dc=example,dc=com")
            .searchFilter("(objectClass=person)")
            .build();

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("credentialId is required"));
  }

  @Test
  void testInvalidPageSize() {
    var config =
        LdapSourceDto.Config.builder()
            .ldapUrl("ldap://ldap.example.com:389")
            .credentialId("ldap-cred")
            .baseDn("ou=users,dc=example,dc=com")
            .searchFilter("(objectClass=person)")
            .pageSize(-1)
            .build();

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("pageSize must be positive"));
  }

  @Test
  void testZeroPageSize() {
    var config =
        LdapSourceDto.Config.builder()
            .ldapUrl("ldap://ldap.example.com:389")
            .credentialId("ldap-cred")
            .baseDn("ou=users,dc=example,dc=com")
            .searchFilter("(objectClass=person)")
            .pageSize(0)
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void testInvalidTimeLimitMs() {
    var config =
        LdapSourceDto.Config.builder()
            .ldapUrl("ldap://ldap.example.com:389")
            .credentialId("ldap-cred")
            .baseDn("ou=users,dc=example,dc=com")
            .searchFilter("(objectClass=person)")
            .timeLimitMs(-1)
            .build();

    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("timeLimitMs must be positive"));
  }

  @Test
  void testZeroTimeLimitMs() {
    var config =
        LdapSourceDto.Config.builder()
            .ldapUrl("ldap://ldap.example.com:389")
            .credentialId("ldap-cred")
            .baseDn("ou=users,dc=example,dc=com")
            .searchFilter("(objectClass=person)")
            .timeLimitMs(0)
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
  }
}
