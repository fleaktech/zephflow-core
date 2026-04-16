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

import io.fleak.zephflow.lib.commands.JsonConfigParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LdapSourceDtoTest {

  @Test
  void testConfigDeserialization() {
    JsonConfigParser<LdapSourceDto.Config> parser =
        new JsonConfigParser<>(LdapSourceDto.Config.class);

    Map<String, Object> configMap = new HashMap<>();
    configMap.put("ldapUrl", "ldaps://ldap.example.com:636");
    configMap.put("credentialId", "ldap-cred");
    configMap.put("baseDn", "ou=users,dc=example,dc=com");
    configMap.put("searchFilter", "(objectClass=person)");
    configMap.put("attributes", List.of("cn", "mail", "sn"));
    configMap.put("searchScope", "SUBTREE");
    configMap.put("pageSize", 500);
    configMap.put("timeLimitMs", 60000);

    LdapSourceDto.Config config = parser.parseConfig(configMap);

    assertEquals("ldaps://ldap.example.com:636", config.getLdapUrl());
    assertEquals("ldap-cred", config.getCredentialId());
    assertEquals("ou=users,dc=example,dc=com", config.getBaseDn());
    assertEquals("(objectClass=person)", config.getSearchFilter());
    assertEquals(List.of("cn", "mail", "sn"), config.getAttributes());
    assertEquals(LdapSourceDto.SearchScope.SUBTREE, config.getSearchScope());
    assertEquals(500, config.getPageSize());
    assertEquals(60000, config.getTimeLimitMs());
  }

  @Test
  void testConfigDefaults() {
    JsonConfigParser<LdapSourceDto.Config> parser =
        new JsonConfigParser<>(LdapSourceDto.Config.class);

    Map<String, Object> configMap = new HashMap<>();
    configMap.put("ldapUrl", "ldap://localhost:389");
    configMap.put("credentialId", "cred");
    configMap.put("baseDn", "dc=example,dc=com");
    configMap.put("searchFilter", "(objectClass=*)");

    LdapSourceDto.Config config = parser.parseConfig(configMap);

    assertEquals(LdapSourceDto.DEFAULT_PAGE_SIZE, config.getPageSize());
    assertEquals(LdapSourceDto.DEFAULT_TIME_LIMIT_MS, config.getTimeLimitMs());
    assertEquals(LdapSourceDto.SearchScope.SUBTREE, config.getSearchScope());
    assertNull(config.getAttributes());
  }

  @Test
  void testConfigBuilder() {
    LdapSourceDto.Config config =
        LdapSourceDto.Config.builder()
            .ldapUrl("ldap://localhost:389")
            .credentialId("my-cred")
            .baseDn("dc=test,dc=com")
            .searchFilter("(uid=*)")
            .attributes(List.of("uid", "cn"))
            .searchScope(LdapSourceDto.SearchScope.ONELEVEL)
            .pageSize(100)
            .timeLimitMs(5000)
            .build();

    assertEquals("ldap://localhost:389", config.getLdapUrl());
    assertEquals("my-cred", config.getCredentialId());
    assertEquals("dc=test,dc=com", config.getBaseDn());
    assertEquals("(uid=*)", config.getSearchFilter());
    assertEquals(List.of("uid", "cn"), config.getAttributes());
    assertEquals(LdapSourceDto.SearchScope.ONELEVEL, config.getSearchScope());
    assertEquals(100, config.getPageSize());
    assertEquals(5000, config.getTimeLimitMs());
  }
}
