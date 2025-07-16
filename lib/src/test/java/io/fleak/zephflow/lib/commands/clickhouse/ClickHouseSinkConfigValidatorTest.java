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
package io.fleak.zephflow.lib.commands.clickhouse;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.utils.MiscUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class ClickHouseSinkConfigValidatorTest {

  private ClickHouseConfigValidator validator;
  private JobContext jobContext;

  @BeforeEach
  void setup() {
    validator = new ClickHouseConfigValidator();
    jobContext = mock(JobContext.class);
  }

  @Test
  void shouldThrowWhenCredentialIdIsSetButNotFound() {
    var config = validConfig().credentialId("some-id").build();
    when(MiscUtils.lookupUsernamePasswordCredentialOpt(jobContext, "some-id"))
            .thenReturn(Optional.empty());

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
            validator.validateConfig(config, "node1", jobContext)
    );

    assertEquals("The credentialId is specific but no credentials record was found", ex.getMessage());
  }

  @Test
  void shouldThrowWhenNoCredentialIdAndNoUsername() {
    var config = validConfig().credentialId(null).username(null).build();

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
            validator.validateConfig(config, "node1", jobContext)
    );

    assertEquals("A username and password must be specified", ex.getMessage());
  }

  @Test
  void shouldThrowWhenEndpointIsMissing() {
    var config = validConfig().endpoint("   ").build();

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
            validator.validateConfig(config, "node1", jobContext)
    );

    assertEquals("A clickhouse endpoint must be specified", ex.getMessage());
  }

  @Test
  void shouldThrowWhenDatabaseIsMissing() {
    var config = validConfig().database("   ").build();

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
            validator.validateConfig(config, "node1", jobContext)
    );

    assertEquals("A clickhouse database must be specified", ex.getMessage());
  }

  @Test
  void shouldThrowWhenTableIsMissing() {
    var config = validConfig().table(" ").build();

    RuntimeException ex = assertThrows(RuntimeException.class, () ->
            validator.validateConfig(config, "node1", jobContext)
    );

    assertEquals("A clickhouse table must be specified", ex.getMessage());
  }

  @Test
  void shouldPassWithValidConfigAndCredential() {
    var config = validConfig().build();
    when(MiscUtils.lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId()))
            .thenReturn(Optional.of(mock(UsernamePasswordCredential.class)));

    assertDoesNotThrow(() ->
            validator.validateConfig(config, "node1", jobContext)
    );
  }

  @Test
  void shouldPassWithUsernamePasswordDirectly() {
    var config = validConfig().credentialId(null).username("user").password("pass").build();

    assertDoesNotThrow(() ->
            validator.validateConfig(config, "node1", jobContext)
    );
  }

  private ClickHouseSinkDto.Config.ConfigBuilder validConfig() {
    return ClickHouseSinkDto.Config.builder()
            .username("user")
            .password("pass")
            .endpoint("http://localhost:8123")
            .database("test_db")
            .table("test_table")
            .credentialId("cred-id");
  }
}

