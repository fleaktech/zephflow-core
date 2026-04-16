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

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.naming.Context;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LdapSourceFetcherTest {

  private static InMemoryDirectoryServer server;
  private static int port;

  @BeforeAll
  static void startServer() throws Exception {
    InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
    config.addAdditionalBindCredentials("cn=admin,dc=example,dc=com", "password");
    config.setSchema(null);

    server = new InMemoryDirectoryServer(config);
    server.add("dn: dc=example,dc=com", "objectClass: domain", "dc: example");
    server.add("dn: ou=users,dc=example,dc=com", "objectClass: organizationalUnit", "ou: users");
    server.add(
        "dn: uid=jdoe,ou=users,dc=example,dc=com",
        "objectClass: inetOrgPerson",
        "uid: jdoe",
        "cn: John Doe",
        "sn: Doe",
        "mail: jdoe@example.com");
    server.add(
        "dn: uid=asmith,ou=users,dc=example,dc=com",
        "objectClass: inetOrgPerson",
        "uid: asmith",
        "cn: Alice Smith",
        "sn: Smith",
        "mail: asmith@example.com");
    server.startListening();
    port = server.getListenPort();
  }

  @AfterAll
  static void stopServer() {
    if (server != null) {
      server.shutDown(true);
    }
  }

  private LdapContext createContext() throws Exception {
    Hashtable<String, String> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, "ldap://localhost:" + port);
    env.put(Context.SECURITY_AUTHENTICATION, "simple");
    env.put(Context.SECURITY_PRINCIPAL, "cn=admin,dc=example,dc=com");
    env.put(Context.SECURITY_CREDENTIALS, "password");
    return new InitialLdapContext(env, null);
  }

  @Test
  void testFetchReturnsMatchingEntries() throws Exception {
    LdapContext ctx = createContext();
    SearchControls controls = new SearchControls();
    controls.setSearchScope(SearchControls.SUBTREE_SCOPE);

    LdapSourceFetcher fetcher =
        new LdapSourceFetcher(
            ctx, "ou=users,dc=example,dc=com", "(objectClass=inetOrgPerson)", controls, 1000);

    List<LdapEntry> entries = fetcher.fetch();

    assertEquals(2, entries.size());
    Set<String> dns = entries.stream().map(LdapEntry::dn).collect(Collectors.toSet());
    assertTrue(dns.contains("uid=jdoe,ou=users,dc=example,dc=com"));
    assertTrue(dns.contains("uid=asmith,ou=users,dc=example,dc=com"));

    fetcher.close();
  }

  @Test
  void testAttributeFiltering() throws Exception {
    LdapContext ctx = createContext();
    SearchControls controls = new SearchControls();
    controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    controls.setReturningAttributes(new String[] {"cn", "mail"});

    LdapSourceFetcher fetcher =
        new LdapSourceFetcher(
            ctx, "ou=users,dc=example,dc=com", "(objectClass=inetOrgPerson)", controls, 1000);

    List<LdapEntry> entries = fetcher.fetch();

    assertEquals(2, entries.size());
    for (LdapEntry entry : entries) {
      assertTrue(entry.attributes().containsKey("cn"));
      assertTrue(entry.attributes().containsKey("mail"));
      assertFalse(entry.attributes().containsKey("objectClass"));
    }

    fetcher.close();
  }

  @Test
  void testIsExhaustedAfterAllResults() throws Exception {
    LdapContext ctx = createContext();
    SearchControls controls = new SearchControls();
    controls.setSearchScope(SearchControls.SUBTREE_SCOPE);

    LdapSourceFetcher fetcher =
        new LdapSourceFetcher(
            ctx, "ou=users,dc=example,dc=com", "(objectClass=inetOrgPerson)", controls, 1000);

    assertFalse(fetcher.isExhausted());
    fetcher.fetch();
    assertTrue(fetcher.isExhausted());

    List<LdapEntry> emptyResult = fetcher.fetch();
    assertTrue(emptyResult.isEmpty());

    fetcher.close();
  }

  @Test
  void testPagedResults() throws Exception {
    InMemoryDirectoryServerConfig pagedConfig =
        new InMemoryDirectoryServerConfig("dc=paged,dc=com");
    pagedConfig.addAdditionalBindCredentials("cn=admin,dc=paged,dc=com", "password");
    pagedConfig.setSchema(null);

    InMemoryDirectoryServer pagedServer = new InMemoryDirectoryServer(pagedConfig);
    pagedServer.add("dn: dc=paged,dc=com", "objectClass: domain", "dc: paged");
    pagedServer.add(
        "dn: ou=people,dc=paged,dc=com", "objectClass: organizationalUnit", "ou: people");

    for (int i = 0; i < 10; i++) {
      pagedServer.add(
          String.format("dn: uid=user%d,ou=people,dc=paged,dc=com", i),
          "objectClass: inetOrgPerson",
          "uid: user" + i,
          "cn: User " + i,
          "sn: Last" + i,
          "mail: user" + i + "@example.com");
    }
    pagedServer.startListening();

    try {
      int pagedPort = pagedServer.getListenPort();
      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env.put(Context.PROVIDER_URL, "ldap://localhost:" + pagedPort);
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
      env.put(Context.SECURITY_PRINCIPAL, "cn=admin,dc=paged,dc=com");
      env.put(Context.SECURITY_CREDENTIALS, "password");
      LdapContext ctx = new InitialLdapContext(env, null);

      SearchControls controls = new SearchControls();
      controls.setSearchScope(SearchControls.SUBTREE_SCOPE);

      LdapSourceFetcher fetcher =
          new LdapSourceFetcher(
              ctx, "ou=people,dc=paged,dc=com", "(objectClass=inetOrgPerson)", controls, 3);

      int totalEntries = 0;
      int fetchCount = 0;
      while (!fetcher.isExhausted()) {
        List<LdapEntry> batch = fetcher.fetch();
        totalEntries += batch.size();
        fetchCount++;
      }

      assertEquals(10, totalEntries);
      assertTrue(fetchCount > 1);

      fetcher.close();
    } finally {
      pagedServer.shutDown(true);
    }
  }

  @Test
  void testSearchScopeOnelevel() throws Exception {
    LdapContext ctx = createContext();
    SearchControls controls = new SearchControls();
    controls.setSearchScope(SearchControls.ONELEVEL_SCOPE);

    LdapSourceFetcher fetcher =
        new LdapSourceFetcher(
            ctx, "dc=example,dc=com", "(objectClass=organizationalUnit)", controls, 1000);

    List<LdapEntry> entries = fetcher.fetch();

    assertEquals(1, entries.size());
    assertEquals("ou=users,dc=example,dc=com", entries.get(0).dn());

    fetcher.close();
  }

  @Test
  void testSearchScopeObject() throws Exception {
    LdapContext ctx = createContext();
    SearchControls controls = new SearchControls();
    controls.setSearchScope(SearchControls.OBJECT_SCOPE);

    LdapSourceFetcher fetcher =
        new LdapSourceFetcher(
            ctx,
            "uid=jdoe,ou=users,dc=example,dc=com",
            "(objectClass=inetOrgPerson)",
            controls,
            1000);

    List<LdapEntry> entries = fetcher.fetch();

    assertEquals(1, entries.size());
    assertEquals("uid=jdoe,ou=users,dc=example,dc=com", entries.get(0).dn());
    assertTrue(entries.get(0).attributes().containsKey("cn"));
    assertEquals("John Doe", entries.get(0).attributes().get("cn").get(0));

    fetcher.close();
  }
}
