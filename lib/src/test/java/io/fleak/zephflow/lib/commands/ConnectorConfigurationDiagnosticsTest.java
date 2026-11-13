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
package io.fleak.zephflow.lib.commands;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.lib.commands.ConnectorConfigurationTestSupport.valid;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.JobContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class ConnectorConfigurationDiagnosticsTest {
  static Stream<Arguments> requiredFields() {
    List<Arguments> cases = new ArrayList<>();
    Map.ofEntries(
            Map.entry("activemqsource", "brokerUrl destination"),
            Map.entry("kafkasource", "broker topic groupId"),
            Map.entry("kinesissource", "applicationName regionStr streamName"),
            Map.entry("mqttsource", "brokerUrl topicFilter clientId"),
            Map.entry("pubsubsource", "subscription"),
            Map.entry("sqssource", "queueUrl regionStr"),
            Map.entry("s3rtsource", "queueUrl regionStr"),
            Map.entry("pubsubsink", "topic"),
            Map.entry("sqssink", "queueUrl regionStr"),
            Map.entry("jdbcsource", "jdbcUrl query"),
            Map.entry("jdbcsink", "jdbcUrl tableName"),
            Map.entry("timescaledbsink", "jdbcUrl tableName timeColumn"),
            Map.entry("imapsource", "host credentialId"),
            Map.entry(
                "smtpsink",
                "host credentialId fromAddress toTemplate subjectTemplate bodyTemplate"),
            Map.entry("ldapsource", "ldapUrl baseDn searchFilter credentialId"),
            Map.entry("elasticsearchsource", "host index"),
            Map.entry("elasticsearchsink", "host index"),
            Map.entry("influxdbsink", "url org bucket"),
            Map.entry("clickhousesink", "endpoint database table"),
            Map.entry("syslogudp", "host"),
            Map.entry("splunksource", "splunkUrl searchQuery credentialId"),
            Map.entry("splunkhecsink", "hecUrl credentialId"),
            Map.entry("gcssink", "bucketName"),
            Map.entry("azureblobsink", "containerName"),
            Map.entry("azuremonitorsource", "workspaceId tenantId kqlQuery credentialId"),
            Map.entry(
                "azuremonitorsink", "tenantId dceEndpoint dcrImmutableId streamName credentialId"),
            Map.entry("azureeventhubsource", "eventHubName consumerGroup checkpointContainerName"),
            Map.entry("azureiothubsource", "eventHubName consumerGroup checkpointContainerName"),
            Map.entry("azureeventhubsink", "eventHubName"),
            Map.entry("kafkasink", "broker topic"),
            Map.entry("fssource", "backend root"))
        .forEach(
            (operator, fields) ->
                Stream.of(fields.split(" "))
                    .forEach(field -> cases.add(Arguments.of(operator, field))));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("requiredFields")
  void locatesRequiredNonBlankFieldsThroughTheActualFactory(String operator, String field) {
    var config = valid(operator);
    assertDoesNotThrow(() -> validate(operator, config));
    config.put(field, " \t");
    assertFailure(operator, config, List.of(of(field)), "blank");
  }

  static Stream<Arguments> numericLimits() {
    List<Arguments> cases = new ArrayList<>();
    Map.ofEntries(
            Map.entry("activemqsource", "commitBatchSize commitIntervalMs"),
            Map.entry("kafkasource", "commitBatchSize commitIntervalMs"),
            Map.entry("mqttsource", "maxBatchSize receiveQueueCapacity receiveTimeoutMs"),
            Map.entry("jdbcsource", "pollIntervalMs fetchSize"),
            Map.entry("jdbcsink", "batchSize"),
            Map.entry("timescaledbsink", "batchSize"),
            Map.entry("imapsource", "port maxMessages pollIntervalMs"),
            Map.entry("smtpsink", "port"),
            Map.entry("ldapsource", "pageSize timeLimitMs"),
            Map.entry("elasticsearchsource", "batchSize"),
            Map.entry("elasticsearchsink", "batchSize"),
            Map.entry("influxdbsink", "batchSize"),
            Map.entry("syslogudp", "port bufferSize queueCapacity"),
            Map.entry("splunkhecsink", "batchSize"),
            Map.entry("gcssink", "batchSize"),
            Map.entry("azureblobsink", "batchSize"),
            Map.entry("azuremonitorsource", "batchSize"),
            Map.entry("azuremonitorsink", "batchSize"),
            Map.entry(
                "azureeventhubsource",
                "maxBufferedEvents maxEventsPerFetch commitBatchSize commitIntervalMs"),
            Map.entry(
                "azureiothubsource",
                "maxBufferedEvents maxEventsPerFetch commitBatchSize commitIntervalMs"),
            Map.entry("s3rtsource", "maxObjectSizeBytes maxRetries"),
            Map.entry("s3sink", "batchSize"),
            Map.entry("fssource", "maxFileBytes chunkSizeBytes maxFilesPerRun localSpoolMaxBytes"))
        .forEach(
            (operator, fields) ->
                Stream.of(fields.split(" "))
                    .forEach(field -> cases.add(Arguments.of(operator, field, 0, 1))));
    return Stream.concat(cases.stream(), boundedNumericLimits());
  }

  private static Stream<Arguments> boundedNumericLimits() {
    List<Arguments> cases = new ArrayList<>();
    for (String operator : List.of("sqssource", "s3rtsource")) {
      cases.add(Arguments.of(operator, "maxNumberOfMessages", 0, 1));
      cases.add(Arguments.of(operator, "maxNumberOfMessages", 11, 10));
      cases.add(Arguments.of(operator, "waitTimeSeconds", -1, 0));
      cases.add(Arguments.of(operator, "waitTimeSeconds", 21, 20));
      cases.add(Arguments.of(operator, "visibilityTimeoutSeconds", -1, 0));
    }
    cases.add(Arguments.of("pubsubsource", "maxMessages", 0, 1));
    cases.add(Arguments.of("pubsubsource", "maxMessages", 1001, 1000));
    cases.add(Arguments.of("pubsubsource", "ackDeadlineExtensionSeconds", -1, 0));
    cases.add(Arguments.of("pubsubsource", "ackDeadlineExtensionSeconds", 601, 600));
    cases.add(Arguments.of("pubsubsink", "batchSize", 0, 1));
    cases.add(Arguments.of("pubsubsink", "batchSize", 1001, 1000));
    cases.add(Arguments.of("sqssink", "batchSize", 0, 1));
    cases.add(Arguments.of("sqssink", "batchSize", 11, 10));
    cases.add(Arguments.of("mqttsource", "qos", -1, 0));
    cases.add(Arguments.of("mqttsource", "qos", 3, 2));
    cases.add(Arguments.of("syslogudp", "port", 65536, 65535));
    cases.add(Arguments.of("splunksource", "jobInitTimeoutMs", -1, 0));
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("numericLimits")
  void locatesNumericBoundsAndAcceptsTheBoundary(
      String operator, String field, int rejected, int accepted) {
    var config = valid(operator);
    config.put(field, accepted);
    assertDoesNotThrow(() -> validate(operator, config));
    config.put(field, rejected);
    assertFailure(operator, config, List.of(of(field)), "must");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"kafkasource", "activemqsource", "azureeventhubsource", "azureiothubsource"})
  void retainsConditionalBatchValidation(String operator) {
    var config = valid(operator);
    config.put("commitStrategy", "NONE");
    config.put("commitBatchSize", 0);
    config.put("commitIntervalMs", 0);
    assertDoesNotThrow(() -> validate(operator, config));
  }

  @ParameterizedTest
  @ValueSource(strings = {"kafkasink", "s3sink", "splunkhecsink"})
  void retainsStoreAndForwardConditions(String operator) {
    for (String field : List.of("localStoreMaxBytes", "forwardRetryIntervalMillis")) {
      var config = valid(operator);
      config.put(field, 0);
      assertDoesNotThrow(() -> validate(operator, config));
      config.put("storeAndForwardEnabled", true);
      assertFailure(operator, config, List.of(of(field)), "greater than 0");
      config.put(field, 1);
      assertDoesNotThrow(() -> validate(operator, config));
    }
  }

  static Stream<Arguments> pathFields() {
    return Stream.of(
        Arguments.of("pubsubsink", "orderingKeyExpression"),
        Arguments.of("sqssink", "messageGroupIdExpression"),
        Arguments.of("sqssink", "deduplicationIdExpression"),
        Arguments.of("kinesissink", "partitionKeyFieldExpressionStr"),
        Arguments.of("kafkasink", "partitionKeyFieldExpressionStr"),
        Arguments.of("azureeventhubsink", "partitionKeyFieldExpressionStr"));
  }

  @ParameterizedTest
  @MethodSource("pathFields")
  void locatesPathSyntaxAndTrailingInput(String operator, String field) {
    var config = valid(operator);
    config.put(field, "$.message");
    assertDoesNotThrow(() -> validate(operator, config));
    for (String invalid :
        List.of("do-not-copy-this", "$.message extra", "$[9999999999999999999999999999]")) {
      config.put(field, invalid);
      var failure = assertFailure(operator, config, List.of(of(field)), "line");
      assertEquals(RuntimeException.class, failure.originalFailure().getClass());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "activemqsource",
        "kafkasource",
        "kinesissource",
        "mqttsource",
        "pubsubsource",
        "sqssource",
        "s3rtsource",
        "stdin",
        "fssource",
        "azureeventhubsource",
        "azureiothubsource"
      })
  void prefixesSourceFormatErrorsOnce(String operator) {
    var config = valid(operator);
    assertDoesNotThrow(() -> validate(operator, config));
    config.put("decoding", Map.of("format", Map.of("type", "json")));
    assertFailure(
        operator, config, List.of(of("decoding", "format", "mode")), "object, array, lines");
    config.remove("decoding");
    assertFailure(operator, config, List.of(of("decoding")), "required");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "kafkasink",
        "kinesissink",
        "s3sink",
        "stdout",
        "sqssink",
        "pubsubsink",
        "splunkhecsink",
        "azureeventhubsink"
      })
  void prefixesSinkFormatErrorsOnce(String operator) {
    var config = valid(operator);
    assertDoesNotThrow(() -> validate(operator, config));
    config.put("encoding", Map.of("format", Map.of("type", "json")));
    assertFailure(
        operator, config, List.of(of("encoding", "format", "mode")), "Allowed JSON modes");
    config.remove("encoding");
    if (List.of("sqssink", "pubsubsink", "splunkhecsink").contains(operator))
      assertDoesNotThrow(() -> validate(operator, config));
    else assertFailure(operator, config, List.of(of("encoding")), "required");
  }

  @Test
  void missingActiveMqBrokerTypeListsAcceptedValues() {
    var config = valid("activemqsource");
    config.remove("brokerType");
    var failure = assertFailure("activemqsource", config, List.of(of("brokerType")), "CLASSIC");
    assertTrue(failure.issues().getFirst().message().contains("ARTEMIS"));
    assertEquals(NullPointerException.class, failure.originalFailure().getClass());
    assertEquals("no brokerType is provided", failure.originalFailure().getMessage());
    for (String brokerType : List.of("CLASSIC", "ARTEMIS")) {
      config.put("brokerType", brokerType);
      assertDoesNotThrow(() -> validate("activemqsource", config));
    }
  }

  @Test
  void retainsConditionalSourceRequirementsAndStateFailureTypes() {
    var activeMq = valid("activemqsource");
    activeMq.put("destinationType", "TOPIC");
    assertFailure("activemqsource", activeMq, List.of(of("clientId")), "durable topic");
    activeMq.put("clientId", "test");
    assertFailure("activemqsource", activeMq, List.of(of("subscriptionName")), "durable topic");
    activeMq.put("subscriptionName", "events");
    assertDoesNotThrow(() -> validate("activemqsource", activeMq));

    var mqtt = valid("mqttsource");
    mqtt.put("useTls", true);
    assertFailure("mqttsource", mqtt, List.of(of("brokerUrl")), "ssl:// or wss://");
    mqtt.put("brokerUrl", "ssl://localhost:8883");
    assertDoesNotThrow(() -> validate("mqttsource", mqtt));

    var kinesis = valid("kinesissource");
    kinesis.put("initialPosition", "AT_TIMESTAMP");
    var failure =
        assertFailure(
            "kinesissource", kinesis, List.of(of("initialPositionTimestamp")), "required");
    assertEquals(IllegalStateException.class, failure.originalFailure().getClass());
    kinesis.put("initialPositionTimestamp", 1000L);
    assertDoesNotThrow(() -> validate("kinesissource", kinesis));
    kinesis.put("initialPosition", "LATEST");
    assertFailure("kinesissource", kinesis, List.of(of("initialPositionTimestamp")), "absent");
    kinesis.remove("initialPosition");
    assertFailure("kinesissource", kinesis, List.of(of("initialPositionTimestamp")), "absent");
    kinesis.remove("initialPositionTimestamp");
    assertDoesNotThrow(() -> validate("kinesissource", kinesis));
  }

  @Test
  void locatesJdbcAndQueueRequirementsWithoutAddingThemToOtherModes() {
    var source = valid("jdbcsource");
    source.put("watermarkColumn", "id");
    assertFailure("jdbcsource", source, List.of(of("query")), ":watermark");
    source.put("query", "select * from events where id > :watermark");
    assertDoesNotThrow(() -> validate("jdbcsource", source));
    for (String operator : List.of("jdbcsink", "timescaledbsink")) {
      var sink = valid(operator);
      sink.put("writeMode", "UPSERT");
      assertFailure(operator, sink, List.of(of("upsertKeyColumns")), "UPSERT");
      sink.put("upsertKeyColumns", "id");
      assertDoesNotThrow(() -> validate(operator, sink));
    }
    var queue = valid("sqssink");
    queue.put("queueUrl", "https://sqs.us-east-1.amazonaws.com/123/events.fifo");
    assertFailure("sqssink", queue, List.of(of("messageGroupIdExpression")), "FIFO");
    queue.put("messageGroupIdExpression", "$.id");
    assertDoesNotThrow(() -> validate("sqssink", queue));
  }

  @Test
  void describesRelationsBetweenFields() {
    var influx = valid("influxdbsink");
    influx.put("measurementField", "event");
    assertFailure(
        "influxdbsink", influx, List.of(of("measurement"), of("measurementField")), "exactly one");
    influx.remove("measurement");
    assertDoesNotThrow(() -> validate("influxdbsink", influx));
    influx.remove("measurementField");
    assertFailure(
        "influxdbsink", influx, List.of(of("measurement"), of("measurementField")), "exactly one");

    var imap = valid("imapsource");
    imap.put("markAsRead", true);
    imap.put("searchCriteria", " seen ");
    assertFailure(
        "imapsource", imap, List.of(of("searchCriteria"), of("markAsRead")), "incompatible");
    imap.put("markAsRead", false);
    assertDoesNotThrow(() -> validate("imapsource", imap));

    var blob = valid("azureblobsink");
    blob.remove("connectionString");
    assertFailure(
        "azureblobsink",
        blob,
        List.of(of("connectionString"), of("credentialId")),
        "must be provided");
    blob.put("credentialId", "test-credential");
    assertDoesNotThrow(() -> validate("azureblobsink", blob));
  }

  @ParameterizedTest
  @ValueSource(strings = {"azureeventhubsource", "azureiothubsource", "azureeventhubsink"})
  void preservesAlternativeAzureAuthenticationAndItsDependentFields(String operator) {
    var config = valid(operator);
    config.remove("connectionString");
    var authPaths = List.of(of("connectionString"), of("fullyQualifiedNamespace"));
    assertFailure(operator, config, authPaths, "requires either");
    config.put("fullyQualifiedNamespace", "example.servicebus.windows.net");
    assertDoesNotThrow(() -> validate(operator, config));
    config.put("connectionString", "test-connection");
    assertFailure(operator, config, authPaths, "only one");
    config.remove("connectionString");
    config.put("tenantId", "test-tenant");
    assertFailure(
        operator,
        config,
        List.of(of("tenantId"), of("clientId"), of("clientSecret")),
        "requires all");
    config.put("clientId", "test-client");
    config.put("clientSecret", "do-not-copy-this");
    assertDoesNotThrow(() -> validate(operator, config));
    config.remove("fullyQualifiedNamespace");
    config.put("connectionString", "test-connection");
    assertFailure(
        operator,
        config,
        List.of(of("fullyQualifiedNamespace")),
        "require fullyQualifiedNamespace");
  }

  @ParameterizedTest
  @ValueSource(strings = {"azureeventhubsource", "azureiothubsource"})
  void locatesBothCheckpointAuthenticationFields(String operator) {
    var config = valid(operator);
    config.remove("checkpointStorageConnectionString");
    var paths = List.of(of("checkpointStorageConnectionString"), of("checkpointStorageEndpoint"));
    assertFailure(operator, config, paths, "requires either");
    config.put("checkpointStorageEndpoint", "https://example.blob.core.windows.net");
    assertDoesNotThrow(() -> validate(operator, config));
    config.put("checkpointStorageConnectionString", "test-storage");
    assertFailure(operator, config, paths, "only one");
  }

  @Test
  void describesExistingUriCharsetPrefixAndRegionConstraintsWithoutEchoingInput() {
    for (String invalid :
        List.of("https://do-not-copy-this invalid", "ftp://example.test", "https:/no-host")) {
      var config = valid("splunksource");
      config.put("splunkUrl", invalid);
      assertFailure(
          "splunksource",
          config,
          List.of(of("splunkUrl")),
          invalid.contains(" ") ? "URL" : invalid.startsWith("ftp") ? "scheme" : "host");
      var hec = valid("splunkhecsink");
      hec.put("hecUrl", invalid);
      assertFailure(
          "splunkhecsink",
          hec,
          List.of(of("hecUrl")),
          invalid.contains(" ") ? "index" : invalid.startsWith("ftp") ? "scheme" : "host");
    }
    var syslog = valid("syslogudp");
    syslog.put("encoding", "do-not-copy-this");
    assertFailure("syslogudp", syslog, List.of(of("encoding")), "supported charset");
    var ldap = valid("ldapsource");
    ldap.put("ldapUrl", "http://example.test");
    assertFailure("ldapsource", ldap, List.of(of("ldapUrl")), "ldap:// or ldaps://");
    var timescale = valid("timescaledbsink");
    timescale.put("jdbcUrl", "jdbc:mysql://localhost/test");
    assertFailure("timescaledbsink", timescale, List.of(of("jdbcUrl")), "jdbc:postgresql:");
    var monitor = valid("azuremonitorsink");
    monitor.put("dcrImmutableId", "wrong-prefix");
    assertFailure("azuremonitorsink", monitor, List.of(of("dcrImmutableId")), "dcr-");
    var s3 = valid("s3sink");
    s3.put("regionStr", "do-not-copy-this");
    assertFailure("s3sink", s3, List.of(of("regionStr")), "us-east-1");
    s3.put("regionStr", "US-EAST-1");
    assertDoesNotThrow(() -> validate("s3sink", s3));
    s3.put("batching", false);
    assertFailure("s3sink", s3, List.of(of("batching")), "must be true");
  }

  @Test
  void locatesKafkaSecurityAndDeliveryConstraintsIncludingLiteralPropertyKeys() {
    var config = valid("kafkasink");
    config.put("storeAndForwardEnabled", true);
    config.put("deliveryMode", "FIRE_AND_FORGET");
    assertFailure(
        "kafkasink",
        config,
        List.of(of("storeAndForwardEnabled"), of("deliveryMode")),
        "acknowledgements");
    config.put("storeAndForwardEnabled", false);
    assertDoesNotThrow(() -> validate("kafkasink", config));
    config.put("securityProtocol", "do-not-copy-this");
    assertFailure("kafkasink", config, List.of(of("securityProtocol")), "SASL_SSL");
    config.put("securityProtocol", "SASL_SSL");
    assertFailure("kafkasink", config, List.of(of("saslMechanism")), "SCRAM-SHA-256");
    config.put("saslMechanism", "PLAIN");
    assertFailure("kafkasink", config, List.of(of("credentialId")), "required");
    config.put("credentialId", "test-credential");
    assertDoesNotThrow(() -> validate("kafkasink", config));
    config.put("properties", Map.of("sasl.jaas.config", "do-not-copy-this"));
    assertFailure("kafkasink", config, List.of(of("properties", "sasl.jaas.config")), "managed");
    config.remove("securityProtocol");
    assertDoesNotThrow(() -> validate("kafkasink", config));
  }

  @Test
  void locatesFileSourceConstraintsWithoutOpeningFiles() {
    var config = valid("fssource");
    config.put("fileNameRegex", "do-not-copy-this(");
    assertFailure("fssource", config, List.of(of("fileNameRegex")), "index");
    config.put("fileNameRegex", " ");
    assertDoesNotThrow(() -> validate("fssource", config));
    config.put("exactObjectKey", " ");
    assertFailure("fssource", config, List.of(of("exactObjectKey")), "blank");
    config.put("exactObjectKey", "prefix/event.json");
    assertFailure("fssource", config, List.of(of("exactObjectKey")), "backend is s3");
    config.put("backend", "s3");
    assertFailure("fssource", config, List.of(of("root")), "s3://");
    config.put("root", "s3://bucket/prefix");
    assertDoesNotThrow(() -> validate("fssource", config));
    for (String key : List.of("/prefix/event.json", "s3://bucket/prefix/event.json")) {
      config.put("exactObjectKey", key);
      assertFailure("fssource", config, List.of(of("exactObjectKey")), "must not start");
    }
    for (String key : List.of("prefix/", "other/event.json")) {
      config.put("exactObjectKey", key);
      assertFailure("fssource", config, List.of(of("exactObjectKey")), "non-empty object key");
    }
    var stdin = valid("stdin");
    stdin.put(
        "decoding",
        Map.of(
            "format",
            Map.of("type", "text", "framing", Map.of("type", "multiline", "startPattern", "a"))));
    assertFailure(
        "stdin", stdin, List.of(of("decoding", "format", "framing", "type")), "cannot use");
  }

  private static void validate(String operator, Map<String, Object> config) {
    OperatorCommandRegistry.OPERATOR_COMMANDS
        .get(operator)
        .createCommand("subject", JobContext.builder().build())
        .parseAndValidateArg(config);
  }

  private static ConfigurationValidationException assertFailure(
      String operator,
      Map<String, Object> config,
      List<ConfigurationPath> paths,
      String constraint) {
    var snapshot = new HashMap<>(config);
    var failure =
        assertThrows(ConfigurationValidationException.class, () -> validate(operator, config));
    assertEquals(paths, failure.issues().stream().map(ConfigurationIssue::path).toList());
    assertTrue(
        failure.issues().stream().allMatch(issue -> issue.message().contains(constraint)),
        failure.issues().toString());
    assertTrue(
        failure.issues().stream().noneMatch(issue -> issue.message().contains("do-not-copy-this")));
    assertEquals(snapshot, config);
    return failure;
  }
}
