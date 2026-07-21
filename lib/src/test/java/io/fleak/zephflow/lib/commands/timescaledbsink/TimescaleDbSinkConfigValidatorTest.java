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
package io.fleak.zephflow.lib.commands.timescaledbsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.jdbcsink.JdbcSinkDto;
import org.junit.jupiter.api.Test;

class TimescaleDbSinkConfigValidatorTest {

  private final TimescaleDbSinkConfigValidator validator = new TimescaleDbSinkConfigValidator();
  private final JobContext jobContext = mock(JobContext.class);

  private static TimescaleDbSinkDto.Config.ConfigBuilder validConfig() {
    return TimescaleDbSinkDto.Config.builder()
        .jdbcUrl("jdbc:postgresql://localhost:5432/tsdb")
        .tableName("metrics")
        .timeColumn("ts");
  }

  private void validate(TimescaleDbSinkDto.Config config) {
    validator.validateConfig(config, "node", jobContext);
  }

  @Test
  void acceptsValidConfig() {
    assertDoesNotThrow(() -> validate(validConfig().build()));
  }

  @Test
  void rejectsMissingJdbcUrl() {
    assertThrows(
        IllegalArgumentException.class, () -> validate(validConfig().jdbcUrl(" ").build()));
  }

  @Test
  void rejectsNonPostgresJdbcUrl() {
    var config = validConfig().jdbcUrl("jdbc:mysql://localhost/tsdb").build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsMissingTableName() {
    assertThrows(
        IllegalArgumentException.class, () -> validate(validConfig().tableName(null).build()));
  }

  @Test
  void rejectsMissingTimeColumn() {
    assertThrows(
        IllegalArgumentException.class, () -> validate(validConfig().timeColumn("").build()));
  }

  @Test
  void rejectsUpsertWithoutKeyColumns() {
    var config = validConfig().writeMode(JdbcSinkDto.WriteMode.UPSERT).build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void acceptsUpsertWithKeyColumns() {
    var config =
        validConfig().writeMode(JdbcSinkDto.WriteMode.UPSERT).upsertKeyColumns("ts,host").build();
    assertDoesNotThrow(() -> validate(config));
  }

  @Test
  void rejectsNonPositiveBatchSize() {
    assertThrows(
        IllegalArgumentException.class, () -> validate(validConfig().batchSize(0).build()));
  }
}
