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
package io.fleak.zephflow.lib.commands.zerobussink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.zerobussink.ZerobusSinkDto.Config;
import io.fleak.zephflow.lib.credentials.DatabricksCredential;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZerobusSinkConfigValidatorTest {

  private ZerobusSinkConfigValidator validator;
  private JobContext jobContext;
  private DatabricksCredential credential;

  private static final Map<String, Object> VALID_AVRO_SCHEMA =
      Map.of(
          "type", "record",
          "name", "TestRecord",
          "fields",
              List.of(
                  Map.of("name", "id", "type", "int"), Map.of("name", "name", "type", "string")));

  private static final String ENDPOINT = "https://1234.zerobus.us-west-2.cloud.databricks.com";

  @BeforeEach
  void setUp() {
    validator = new ZerobusSinkConfigValidator();
    jobContext = mock(JobContext.class);
    credential =
        DatabricksCredential.builder()
            .host("https://test.databricks.com")
            .clientId("client-id")
            .clientSecret("client-secret")
            .build();
  }

  private Config.ConfigBuilder validConfigBuilder() {
    when(jobContext.getOtherProperties()).thenReturn(Map.of("test-credential", credential));
    return Config.builder()
        .databricksCredentialId("test-credential")
        .zerobusEndpoint(ENDPOINT)
        .tableName("catalog.schema.table")
        .avroSchema(VALID_AVRO_SCHEMA);
  }

  @Test
  void validProtobufConfig() {
    Config config = validConfigBuilder().build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "node", jobContext));
  }

  @Test
  void validJsonConfig() {
    Config config = validConfigBuilder().encodingType(ZerobusSinkDto.ENCODING_JSON).build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "node", jobContext));
  }

  @Test
  void jsonConfigWithoutAvroSchemaIsValid() {
    // JSON mode does not use the Avro schema (no protobuf descriptor is built), so a missing
    // schema must not be a config error.
    Config config =
        validConfigBuilder().encodingType(ZerobusSinkDto.ENCODING_JSON).avroSchema(null).build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "node", jobContext));
  }

  @Test
  void protobufConfigWithoutAvroSchemaFails() {
    Config config = validConfigBuilder().avroSchema(null).build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", jobContext));
    assertTrue(e.getMessage().contains("avroSchema is required"));
  }

  @Test
  void missingCredentialId() {
    when(jobContext.getOtherProperties()).thenReturn(Map.of());
    Config config =
        Config.builder()
            .zerobusEndpoint(ENDPOINT)
            .tableName("catalog.schema.table")
            .avroSchema(VALID_AVRO_SCHEMA)
            .build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", jobContext));
    assertTrue(e.getMessage().contains("databricksCredentialId is required"));
  }

  @Test
  void credentialNotInJobContext() {
    when(jobContext.getOtherProperties()).thenReturn(Map.of());
    Config config =
        Config.builder()
            .databricksCredentialId("missing")
            .zerobusEndpoint(ENDPOINT)
            .tableName("catalog.schema.table")
            .avroSchema(VALID_AVRO_SCHEMA)
            .build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", jobContext));
    assertTrue(e.getMessage().contains("no credential found"));
  }

  @Test
  void missingZerobusEndpoint() {
    Config config = validConfigBuilder().zerobusEndpoint("").build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", jobContext));
    assertTrue(e.getMessage().contains("zerobusEndpoint is required"));
  }

  @Test
  void nonHttpsZerobusEndpoint() {
    Config config = validConfigBuilder().zerobusEndpoint("ftp://foo.example.com").build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", jobContext));
    assertTrue(e.getMessage().contains("zerobusEndpoint must be an https URL"));
  }

  @Test
  void privateLinkEndpointAccepted() {
    // No ".zerobus." host-shape heuristic: private-link / custom-DNS https endpoints are valid.
    Config config =
        validConfigBuilder().zerobusEndpoint("https://my-private-ingest.internal.example").build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "node", jobContext));
  }

  @Test
  void invalidTableNameTwoParts() {
    Config config = validConfigBuilder().tableName("schema.table").build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", jobContext));
    assertTrue(e.getMessage().contains("tableName must match pattern"));
  }

  @Test
  void invalidEncodingType() {
    Config config = validConfigBuilder().encodingType("avro").build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", jobContext));
    assertTrue(e.getMessage().contains("encodingType"));
  }

  @Test
  void missingAvroSchema() {
    Config config = validConfigBuilder().avroSchema(Map.of()).build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", jobContext));
    assertTrue(e.getMessage().contains("avroSchema is required"));
  }

  @Test
  void endpointWithSurroundingWhitespaceFails() {
    // The flusher hands the raw value to the SDK, so a padded-but-otherwise-valid endpoint must be
    // rejected rather than silently trimmed.
    Config config = validConfigBuilder().zerobusEndpoint(" " + ENDPOINT + " ").build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", jobContext));
    assertTrue(e.getMessage().contains("whitespace"));
  }

  @Test
  void tableNameWithSurroundingWhitespaceFails() {
    Config config = validConfigBuilder().tableName(" catalog.schema.table ").build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", jobContext));
    assertTrue(e.getMessage().contains("whitespace"));
  }

  @Test
  void tableNameWithBlankPartFails() {
    Config config = validConfigBuilder().tableName("catalog. .table").build();
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", jobContext));
    assertTrue(e.getMessage().contains("tableName must match pattern"));
  }
}
