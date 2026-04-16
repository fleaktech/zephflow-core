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

import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import java.io.IOException;
import java.util.*;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.PagedResultsResponseControl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LdapSourceFetcher implements Fetcher<LdapEntry> {

  private final LdapContext ldapContext;
  private final String baseDn;
  private final String searchFilter;
  private final SearchControls searchControls;
  private final int pageSize;

  private boolean exhausted = false;
  private byte[] pageCookie = null;

  public LdapSourceFetcher(
      LdapContext ldapContext,
      String baseDn,
      String searchFilter,
      SearchControls searchControls,
      int pageSize) {
    this.ldapContext = ldapContext;
    this.baseDn = baseDn;
    this.searchFilter = searchFilter;
    this.searchControls = searchControls;
    this.pageSize = pageSize;
  }

  @Override
  public List<LdapEntry> fetch() {
    if (exhausted) {
      return List.of();
    }

    try {
      ldapContext.setRequestControls(
          new Control[] {new PagedResultsControl(pageSize, pageCookie, Control.CRITICAL)});

      NamingEnumeration<SearchResult> results =
          ldapContext.search(baseDn, searchFilter, searchControls);

      List<LdapEntry> entries = new ArrayList<>();
      while (results.hasMore()) {
        SearchResult result = results.next();
        String dn = result.getNameInNamespace();
        Map<String, List<String>> attributeMap = extractAttributes(result.getAttributes());
        entries.add(new LdapEntry(dn, attributeMap));
      }
      results.close();

      pageCookie = extractPageCookie(ldapContext.getResponseControls());
      if (pageCookie == null || pageCookie.length == 0) {
        exhausted = true;
      }

      log.debug("Fetched {} LDAP entries", entries.size());
      return entries;
    } catch (NamingException | IOException e) {
      throw new RuntimeException("Failed to fetch LDAP entries", e);
    }
  }

  @Override
  public boolean isExhausted() {
    return exhausted;
  }

  @Override
  public CommitStrategy commitStrategy() {
    return NoCommitStrategy.INSTANCE;
  }

  @Override
  public void close() throws IOException {
    if (ldapContext != null) {
      try {
        ldapContext.close();
      } catch (NamingException e) {
        throw new IOException("Failed to close LDAP context", e);
      }
    }
  }

  private Map<String, List<String>> extractAttributes(Attributes attributes)
      throws NamingException {
    Map<String, List<String>> attributeMap = new LinkedHashMap<>();
    NamingEnumeration<? extends Attribute> attrEnum = attributes.getAll();
    while (attrEnum.hasMore()) {
      Attribute attr = attrEnum.next();
      List<String> values = new ArrayList<>();
      NamingEnumeration<?> valueEnum = attr.getAll();
      while (valueEnum.hasMore()) {
        Object value = valueEnum.next();
        values.add(value.toString());
      }
      valueEnum.close();
      attributeMap.put(attr.getID(), values);
    }
    attrEnum.close();
    return attributeMap;
  }

  private byte[] extractPageCookie(Control[] responseControls) {
    if (responseControls == null) {
      return null;
    }
    for (Control control : responseControls) {
      if (control instanceof PagedResultsResponseControl pagedResponse) {
        return pagedResponse.getCookie();
      }
    }
    return null;
  }
}
