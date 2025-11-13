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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class SplunkSourceCommandTest {

  @Test
  void testFactoryRegistered() {
    var factory =
        OperatorCommandRegistry.OPERATOR_COMMANDS.get(MiscUtils.COMMAND_NAME_SPLUNK_SOURCE);
    assertNotNull(factory, "SplunkSourceCommandFactory should be registered");
    assertInstanceOf(
        SplunkSourceCommandFactory.class,
        factory,
        "Factory should be instance of SplunkSourceCommandFactory");
  }

  @Test
  void testConfigParsing() {
    var config =
        SplunkSourceDto.Config.builder()
            .splunkUrl("https://splunk.example.com:8089")
            .searchQuery("search index=main | head 10")
            .credentialId("splunk-cred")
            .build();

    var configMap = OBJECT_MAPPER.convertValue(config, new TypeReference<>() {});

    var parsedConfig = OBJECT_MAPPER.convertValue(configMap, SplunkSourceDto.Config.class);

    assertEquals("https://splunk.example.com:8089", parsedConfig.getSplunkUrl());
    assertEquals("search index=main | head 10", parsedConfig.getSearchQuery());
    assertEquals("splunk-cred", parsedConfig.getCredentialId());
  }

  @Test
  void testCommandCreation() {
    var factory =
        OperatorCommandRegistry.OPERATOR_COMMANDS.get(MiscUtils.COMMAND_NAME_SPLUNK_SOURCE);
    var command = factory.createCommand("test-node", TestUtils.JOB_CONTEXT);

    assertNotNull(command);
    assertInstanceOf(SplunkSourceCommand.class, command);
    assertInstanceOf(SourceCommand.class, command);
    assertEquals(SourceCommand.SourceType.BATCH, ((SourceCommand) command).sourceType());
  }

  @Test
  @Disabled(
      "Integration test - requires real Splunk instance. Set env vars: SPLUNK_URL, SPLUNK_USERNAME, SPLUNK_PASSWORD, SPLUNK_SEARCH_QUERY")
  void testSplunkSourceCommandIntegration() throws Exception {
    String splunkUrl = System.getenv("SPLUNK_URL");
    String username = System.getenv("SPLUNK_USERNAME");
    String password = System.getenv("SPLUNK_PASSWORD");
    String searchQuery = System.getenv("SPLUNK_SEARCH_QUERY");

    if (splunkUrl == null || username == null || password == null || searchQuery == null) {
      System.out.println("Skipping integration test - missing environment variables");
      System.out.println(
          "Required: SPLUNK_URL, SPLUNK_USERNAME, SPLUNK_PASSWORD, SPLUNK_SEARCH_QUERY");
      return;
    }

    System.out.println("=".repeat(80));
    System.out.println("Splunk Integration Test");
    System.out.println("=".repeat(80));
    System.out.println("Splunk URL: " + splunkUrl);
    System.out.println("Search query: " + searchQuery);
    System.out.println("=".repeat(80));

    var credential = new UsernamePasswordCredential(username, password);
    var jobContext = TestUtils.buildJobContext(new HashMap<>(Map.of("splunk-cred", credential)));

    var config =
        SplunkSourceDto.Config.builder()
            .splunkUrl(splunkUrl)
            .searchQuery(searchQuery)
            .credentialId("splunk-cred")
            .build();

    var factory =
        OperatorCommandRegistry.OPERATOR_COMMANDS.get(MiscUtils.COMMAND_NAME_SPLUNK_SOURCE);
    var command = (SplunkSourceCommand) factory.createCommand("splunk-test-node", jobContext);

    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());

    var acceptor =
        new SourceEventAcceptor() {
          private int eventCount = 0;

          @Override
          public void accept(List<RecordFleakData> events) {
            for (RecordFleakData event : events) {
              eventCount++;
              System.out.println("\nEvent " + eventCount + ":");
              System.out.println("-".repeat(80));
              System.out.println(toJsonString(event));
            }
          }

          @Override
          public void terminate() {
            System.out.println("\n" + "=".repeat(80));
            System.out.println("Total events fetched: " + eventCount);
            System.out.println("=".repeat(80));
          }
        };

    command.execute("integration-test-user", acceptor);
  }
}
