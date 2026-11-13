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

import java.util.HashMap;
import java.util.Map;

/** Positive controls shared by the connector configuration regression cases. */
final class ConnectorConfigurationTestSupport {
  private static final Map<String, Object> JSON_OBJECT =
      Map.of("format", Map.of("type", "json", "mode", "object"));
  private static final Map<String, Object> JSON_LINES =
      Map.of("format", Map.of("type", "json", "mode", "lines"));

  private static final Map<String, Map<String, Object>> VALID_CONFIGURATIONS =
      Map.ofEntries(
          Map.entry(
              "activemqsource",
              Map.of(
                  "brokerUrl",
                  "tcp://localhost:61616",
                  "brokerType",
                  "CLASSIC",
                  "destination",
                  "events",
                  "decoding",
                  JSON_OBJECT)),
          Map.entry(
              "kafkasource",
              Map.of(
                  "broker",
                  "localhost:9092",
                  "topic",
                  "events",
                  "groupId",
                  "consumer",
                  "decoding",
                  JSON_OBJECT)),
          Map.entry(
              "kinesissource",
              Map.of(
                  "regionStr",
                  "us-east-1",
                  "streamName",
                  "events",
                  "applicationName",
                  "test",
                  "decoding",
                  JSON_OBJECT)),
          Map.entry(
              "mqttsource",
              Map.of(
                  "brokerUrl",
                  "tcp://localhost:1883",
                  "topicFilter",
                  "events",
                  "clientId",
                  "test",
                  "decoding",
                  JSON_OBJECT)),
          Map.entry("pubsubsource", Map.of("subscription", "events", "decoding", JSON_OBJECT)),
          Map.entry(
              "sqssource",
              Map.of(
                  "queueUrl",
                  "https://sqs.us-east-1.amazonaws.com/123/events",
                  "regionStr",
                  "us-east-1",
                  "decoding",
                  JSON_OBJECT)),
          Map.entry(
              "s3rtsource",
              Map.of(
                  "queueUrl",
                  "https://sqs.us-east-1.amazonaws.com/123/events",
                  "regionStr",
                  "us-east-1",
                  "decoding",
                  JSON_OBJECT)),
          Map.entry("pubsubsink", Map.of("topic", "events")),
          Map.entry(
              "sqssink",
              Map.of(
                  "queueUrl",
                  "https://sqs.us-east-1.amazonaws.com/123/events",
                  "regionStr",
                  "us-east-1")),
          Map.entry(
              "jdbcsource",
              Map.of(
                  "jdbcUrl", "jdbc:postgresql://localhost/test", "query", "select * from events")),
          Map.entry(
              "jdbcsink",
              Map.of("jdbcUrl", "jdbc:postgresql://localhost/test", "tableName", "events")),
          Map.entry(
              "timescaledbsink",
              Map.of(
                  "jdbcUrl",
                  "jdbc:postgresql://localhost/test",
                  "tableName",
                  "events",
                  "timeColumn",
                  "timestamp")),
          Map.entry(
              "imapsource", Map.of("host", "mail.example.test", "credentialId", "test-credential")),
          Map.entry(
              "smtpsink",
              Map.of(
                  "host",
                  "mail.example.test",
                  "credentialId",
                  "test-credential",
                  "fromAddress",
                  "from@example.test",
                  "toTemplate",
                  "to@example.test",
                  "subjectTemplate",
                  "subject",
                  "bodyTemplate",
                  "body")),
          Map.entry(
              "ldapsource",
              Map.of(
                  "ldapUrl",
                  "ldap://localhost",
                  "baseDn",
                  "dc=example,dc=test",
                  "searchFilter",
                  "(objectClass=*)",
                  "credentialId",
                  "test-credential")),
          Map.entry("elasticsearchsource", Map.of("host", "localhost:9200", "index", "events")),
          Map.entry("elasticsearchsink", Map.of("host", "localhost:9200", "index", "events")),
          Map.entry(
              "influxdbsink",
              Map.of(
                  "url",
                  "http://localhost:8086",
                  "org",
                  "test",
                  "bucket",
                  "events",
                  "measurement",
                  "event")),
          Map.entry(
              "clickhousesink",
              Map.of("endpoint", "http://localhost:8123", "database", "test", "table", "events")),
          Map.entry("syslogudp", Map.of()),
          Map.entry(
              "splunksource",
              Map.of(
                  "splunkUrl",
                  "https://splunk.example.test",
                  "searchQuery",
                  "search index=events",
                  "credentialId",
                  "test-credential")),
          Map.entry(
              "splunkhecsink",
              Map.of("hecUrl", "https://splunk.example.test", "credentialId", "test-credential")),
          Map.entry("gcssink", Map.of("bucketName", "events")),
          Map.entry(
              "azureblobsink",
              Map.of("containerName", "events", "connectionString", "test-connection")),
          Map.entry(
              "azuremonitorsource",
              Map.of(
                  "workspaceId",
                  "test-workspace",
                  "tenantId",
                  "test-tenant",
                  "kqlQuery",
                  "Events",
                  "credentialId",
                  "test-credential")),
          Map.entry(
              "azuremonitorsink",
              Map.of(
                  "tenantId",
                  "test-tenant",
                  "dceEndpoint",
                  "https://monitor.example.test",
                  "dcrImmutableId",
                  "dcr-test",
                  "streamName",
                  "Custom-Events",
                  "credentialId",
                  "test-credential")),
          Map.entry(
              "azureeventhubsource",
              Map.of(
                  "eventHubName",
                  "events",
                  "consumerGroup",
                  "$Default",
                  "connectionString",
                  "test-connection",
                  "checkpointContainerName",
                  "checkpoints",
                  "checkpointStorageConnectionString",
                  "test-storage",
                  "decoding",
                  JSON_OBJECT)),
          Map.entry(
              "azureiothubsource",
              Map.of(
                  "eventHubName",
                  "events",
                  "consumerGroup",
                  "$Default",
                  "connectionString",
                  "test-connection",
                  "checkpointContainerName",
                  "checkpoints",
                  "checkpointStorageConnectionString",
                  "test-storage",
                  "decoding",
                  JSON_OBJECT)),
          Map.entry(
              "azureeventhubsink",
              Map.of(
                  "eventHubName",
                  "events",
                  "connectionString",
                  "test-connection",
                  "encoding",
                  JSON_OBJECT)),
          Map.entry(
              "kafkasink",
              Map.of("broker", "localhost:9092", "topic", "events", "encoding", JSON_OBJECT)),
          Map.entry("kinesissink", Map.of("encoding", JSON_OBJECT)),
          Map.entry("stdout", Map.of("encoding", JSON_OBJECT)),
          Map.entry(
              "s3sink",
              Map.of(
                  "regionStr",
                  "us-east-1",
                  "bucketName",
                  "events",
                  "keyName",
                  "events",
                  "encoding",
                  JSON_LINES)),
          Map.entry("stdin", Map.of("decoding", JSON_OBJECT)),
          Map.entry(
              "fssource",
              Map.of("backend", "local", "root", "/tmp/events", "decoding", JSON_OBJECT)));

  private ConnectorConfigurationTestSupport() {}

  static Map<String, Object> valid(String operator) {
    var configuration = VALID_CONFIGURATIONS.get(operator);
    if (configuration == null) throw new AssertionError("Missing positive control for " + operator);
    return new HashMap<>(configuration);
  }
}
