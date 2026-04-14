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

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import java.util.Hashtable;
import java.util.Map;
import java.util.Optional;
import javax.naming.Context;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

public class LdapSourceCommand extends SimpleSourceCommand<LdapEntry> {

  public LdapSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    LdapSourceDto.Config config = (LdapSourceDto.Config) commandConfig;

    Fetcher<LdapEntry> fetcher = createLdapFetcher(config, jobContext);
    RawDataEncoder<LdapEntry> encoder = new LdapRawDataEncoder();
    RawDataConverter<LdapEntry> converter = new LdapRawDataConverter();

    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    FleakCounter dataSizeCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_SIZE_COUNT, metricTags);
    FleakCounter inputEventCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter deserializeFailureCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_DESER_ERR_COUNT, metricTags);

    String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
    DlqWriter dlqWriter =
        Optional.of(jobContext)
            .map(JobContext::getDlqConfig)
            .map(c -> DlqWriterFactory.createDlqWriter(c, keyPrefix))
            .orElse(null);
    if (dlqWriter != null) {
      dlqWriter.open();
    }

    return new SourceExecutionContext<>(
        fetcher,
        converter,
        encoder,
        dataSizeCounter,
        inputEventCounter,
        deserializeFailureCounter,
        dlqWriter);
  }

  private Fetcher<LdapEntry> createLdapFetcher(LdapSourceDto.Config config, JobContext jobContext) {
    try {
      UsernamePasswordCredential credential =
          lookupUsernamePasswordCredential(jobContext, config.getCredentialId());

      Hashtable<String, String> env = new Hashtable<>();
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
      env.put(Context.PROVIDER_URL, config.getLdapUrl());
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
      env.put(Context.SECURITY_PRINCIPAL, credential.getUsername());
      env.put(Context.SECURITY_CREDENTIALS, credential.getPassword());

      LdapContext ctx = new InitialLdapContext(env, null);

      SearchControls searchControls = buildSearchControls(config);
      int pageSize =
          config.getPageSize() != null ? config.getPageSize() : LdapSourceDto.DEFAULT_PAGE_SIZE;

      return new LdapSourceFetcher(
          ctx, config.getBaseDn(), config.getSearchFilter(), searchControls, pageSize);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create LDAP fetcher", e);
    }
  }

  private SearchControls buildSearchControls(LdapSourceDto.Config config) {
    SearchControls controls = new SearchControls();

    LdapSourceDto.SearchScope scope =
        config.getSearchScope() != null
            ? config.getSearchScope()
            : LdapSourceDto.SearchScope.SUBTREE;
    controls.setSearchScope(
        switch (scope) {
          case SUBTREE -> SearchControls.SUBTREE_SCOPE;
          case ONELEVEL -> SearchControls.ONELEVEL_SCOPE;
          case OBJECT -> SearchControls.OBJECT_SCOPE;
        });

    if (config.getAttributes() != null && !config.getAttributes().isEmpty()) {
      controls.setReturningAttributes(config.getAttributes().toArray(new String[0]));
    }

    int timeLimitMs =
        config.getTimeLimitMs() != null
            ? config.getTimeLimitMs()
            : LdapSourceDto.DEFAULT_TIME_LIMIT_MS;
    controls.setTimeLimit(timeLimitMs);

    return controls;
  }

  @Override
  public SourceType sourceType() {
    return SourceType.BATCH;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_LDAP_SOURCE;
  }
}
